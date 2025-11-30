import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import threading

from metadata import MetadataDB
from schema import LOG_SCHEMA


def create_record_batch(logs: list, container: str, session: str) -> pa.RecordBatch:
    """
    Convert log entries to Arrow RecordBatch with validation.
    This function performs all validation using Arrow's type system.

    Args:
        logs: List of log dictionaries with 'timestamp', 'level', 'message' fields
        container: Container ID
        session: Session ID

    Returns:
        pa.RecordBatch ready to write

    Raises:
        ValueError: If validation fails (missing fields, invalid types, etc.)
    """
    if not logs:
        raise ValueError("Empty logs array")

    timestamps = []
    levels = []
    messages = []
    containers = []
    sessions = []

    for i, log in enumerate(logs):
        # Validate log is a dictionary
        if not isinstance(log, dict):
            raise ValueError(f"Log entry at index {i} must be an object")

        # Validate required fields exist
        if 'timestamp' not in log:
            raise ValueError(f"Log entry at index {i} missing required field 'timestamp'")
        if 'level' not in log:
            raise ValueError(f"Log entry at index {i} missing required field 'level'")
        if 'message' not in log:
            raise ValueError(f"Log entry at index {i} missing required field 'message'")

        # Parse and validate timestamp (ISO 8601 only)
        ts = log['timestamp']
        if not isinstance(ts, str):
            raise ValueError(f"Log entry at index {i}: timestamp must be an ISO 8601 string, got {type(ts).__name__}")

        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            ts_us = int(dt.timestamp() * 1_000_000)
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Log entry at index {i}: invalid ISO 8601 timestamp format: {ts}") from e

        timestamps.append(ts_us)
        levels.append(str(log['level']))
        messages.append(str(log['message']))
        containers.append(container)
        sessions.append(session)

    # Use Arrow to create the batch - Arrow will validate types
    try:
        return pa.RecordBatch.from_arrays([
            pa.array(timestamps, type=pa.timestamp('us', tz='UTC')),
            pa.array(levels, type=pa.string()),
            pa.array(messages, type=pa.string()),
            pa.array(containers, type=pa.string()),
            pa.array(sessions, type=pa.string()),
        ], schema=LOG_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise ValueError(f"Arrow validation failed: {e}") from e


class BufferManager:
    """
    manages session buffers
    Each session buffer handles buffering logs for a specific container and session.
    Buffers are flushed to Parquet files when they reach a size limit.
    """

    def __init__(self, buffer_dir: Path, archive_dir: Path,
                 metadata_db: MetadataDB,
                 buffer_size_limit: int = 10 * 1024 * 1024):
        self.sessions: Dict[str, 'SessionBuffer'] = {}
        self.buffer_dir = buffer_dir
        self.archive_dir = archive_dir
        self.metadata_db = metadata_db
        self.buffer_size_limit = buffer_size_limit
        self.lock = threading.Lock()

    def get_session_buffer(self, container: str, session: str) -> 'SessionBuffer':
        key = f"{container}_{session}"
        with self.lock:
            if key not in self.sessions:
                self.sessions[key] = SessionBuffer(
                    container, session, self.buffer_dir, self.archive_dir,
                    self.metadata_db,
                    self.buffer_size_limit)
            return self.sessions[key]

    def close_all(self):
        with self.lock:
            for buffer in self.sessions.values():
                buffer.close()


class SessionBuffer:
    """
    Manages buffering logs for a specific container and session.
    Buffers are written to Arrow IPC files and flushed to Parquet when size limit is reached
    and archived. Metadata is recorded in the provided MetadataDB.
    """
    def __init__(self, container: str,
                 session: str,
                 buffer_dir: Path,
                 archive_dir: Path,
                 metadata_db: MetadataDB,
                 buffer_size_limit: int
                 ):
        self.container = container
        self.session = session
        # Fix: Include container in session_dir to avoid collisions
        self.session_dir = buffer_dir / container / f"session_{session}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir = archive_dir / container / f"session_{session}"
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        # Fix: Initialize buffer counter from existing files to avoid overwriting
        self.buffer_counter = self._get_last_buffer_number()
        self.buffer_size_limit = buffer_size_limit
        self.metadata_db = metadata_db
        self.current_writer: Optional[ipc.RecordBatchFileWriter] = None
        self.current_file: Optional[Path] = None
        self.current_sink: Optional[pa.NativeFile] = None
        self.current_size = 0
        self.lock = threading.Lock()

    def _get_last_buffer_number(self) -> int:
        """Get the highest buffer number from existing files"""
        existing = list(self.session_dir.glob("buffer-*.arrow"))
        if not existing:
            return 0
        numbers = []
        for f in existing:
            try:
                num = int(f.stem.split('-')[1])
                numbers.append(num)
            except (IndexError, ValueError):
                continue
        return max(numbers) if numbers else 0

    def _get_next_buffer_path(self) -> Path:
        self.buffer_counter += 1
        return self.session_dir / f"buffer-{self.buffer_counter:04d}.arrow"

    def append_batch(self, batch: pa.RecordBatch):
        """Append an Arrow RecordBatch to the buffer"""
        with self.lock:
            # Initialize writer if needed
            if self.current_writer is None:
                self._init_new_buffer()

            assert self.current_writer is not None

            # Write batch to buffer
            self.current_writer.write_batch(batch)

            # Fix: Track size using batch's memory size as approximation
            # This is faster than disk I/O and close enough for flush threshold
            self.current_size += batch.nbytes

            # Check if we need to flush
            if self.current_size >= self.buffer_size_limit:
                self._flush_buffer()

    def _init_new_buffer(self):
        """Initialize a new Arrow IPC buffer file"""
        self.current_file = self._get_next_buffer_path()
        # Fix: Use output_stream instead of OSFile with mode
        self.current_sink = pa.output_stream(str(self.current_file))
        self.current_writer = ipc.new_file(self.current_sink, LOG_SCHEMA)
        self.current_size = 0

    def _flush_buffer(self, do_async: bool = True):
        """Flush current buffer: convert to Parquet and archive"""
        if self.current_writer is None:
            return

        self.current_writer.close()
        if self.current_sink is not None:
            self.current_sink.close()

        assert self.current_file is not None
        buffer_file = self.current_file

        # Reset for next buffer
        self.current_writer = None
        self.current_sink = None
        self.current_file = None
        self.current_size = 0

        if not do_async:
            assert buffer_file is not None
            self._process_buffer(buffer_file)
            return

        # Process asynchronously
        thread = threading.Thread(
            target=self._process_buffer,
            args=(buffer_file,),
            name=f"flush-{self.container}-{self.session}"
        )
        thread.daemon = True
        thread.start()

    def _process_buffer(self, buffer_file: Path):
        """Async processing: Arrow -> Parquet -> Archive -> Delete"""
        try:
            # Fix: Use memory-mapped I/O for zero-copy reading
            with pa.memory_map(str(buffer_file), 'r') as source:
                reader = ipc.open_file(source)
                table = reader.read_all()

            # Convert to Parquet with optimizations
            parquet_file = buffer_file.with_suffix('.parquet')
            pq.write_table(
                table,
                parquet_file,
                compression='snappy',
                use_dictionary=True,
                data_page_size=65536,
                coerce_timestamps='us'
            )

            # Move to archive directory
            archive_file = self.archive_dir / parquet_file.name
            parquet_file.rename(archive_file)

            # Insert metadata entry
            self._insert_metadata(
                str(archive_file), table.num_rows, archive_file.stat().st_size)

            # Delete local buffer file
            buffer_file.unlink()

            print(
                f"Successfully flushed buffer {buffer_file.name} to archive: {archive_file}")

        except Exception as e:
            print(f"Error processing buffer {buffer_file}: {e}")

    def _insert_metadata(self, archive_path: str, num_rows: int, file_size: int):
        """Insert metadata entry into SQLite database"""
        try:
            self.metadata_db.insert_file_metadata(
                self.container,
                self.session,
                archive_path,
                num_rows,
                file_size
            )
            print(
                f"Metadata inserted for {archive_path}: {num_rows} rows, {file_size} bytes")
        except Exception as e:
            print(f"Error inserting metadata: {e}")

    def close(self):
        """Close and flush current buffer"""
        with self.lock:
            if self.current_writer is not None:
                self._flush_buffer(do_async=False)