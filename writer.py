import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Optional
import threading
import os
import logging
from concurrent.futures import ThreadPoolExecutor

from metadata import MetadataDB
from schema import LOG_SCHEMA

logger = logging.getLogger(__name__)

# Shared thread pool for async buffer flushing (bounded, prevents thread explosion)
FLUSH_POOL = ThreadPoolExecutor(
    max_workers=os.cpu_count() or 4,
    thread_name_prefix="buffer-flush"
)


def create_record_batch(logs: list, container: str, session: str) -> pa.RecordBatch:
    """
    Convert log entries to Arrow RecordBatch with vectorized validation.
    Arrow handles timestamp parsing and type checking internally.

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

    # Validate logs is a list of dicts
    if not all(isinstance(log, dict) for log in logs):
        raise ValueError("All log entries must be dictionaries")

    # Extract required fields vectorized
    try:
        timestamps_raw = [log["timestamp"] for log in logs]
        levels_raw     = [log["level"]     for log in logs]
        messages_raw   = [log["message"]   for log in logs]
    except KeyError as e:
        missing = e.args[0]
        raise ValueError(f"Missing required field '{missing}' in at least one log entry")

    n = len(logs)

    # Validate container and session are strings (not numbers)
    if not isinstance(container, str):
        raise ValueError(f"Container must be a string, got {type(container).__name__}: {container!r}")
    if not isinstance(session, str):
        raise ValueError(f"Session must be a string, got {type(session).__name__}: {session!r}")

    try:
        # Infer type as string first, then cast to timestamp
        # This handles ISO 8601 strings with 'Z' suffix correctly
        timestamps = pa.array(timestamps_raw)  # infer type as string
        timestamps = timestamps.cast(pa.timestamp("us", tz="UTC"))

        levels    = pa.array(levels_raw,   type=pa.string())
        messages  = pa.array(messages_raw, type=pa.string())

        # Constant columns (container/session) repeated
        container_col = pa.array([container] * n, type=pa.string())
        session_col   = pa.array([session]   * n, type=pa.string())

        return pa.RecordBatch.from_arrays(
            [timestamps, levels, messages, container_col, session_col],
            schema=LOG_SCHEMA
        )

    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        # Add detailed error message with sample data for debugging
        error_msg = f"Arrow validation failed: {e}\n"
        error_msg += f"Sample data (first log entry):\n"
        error_msg += f"  timestamp: {timestamps_raw[0]!r} (type: {type(timestamps_raw[0]).__name__})\n"
        error_msg += f"  level: {levels_raw[0]!r} (type: {type(levels_raw[0]).__name__})\n"
        error_msg += f"  message: {messages_raw[0]!r} (type: {type(messages_raw[0]).__name__})\n"
        error_msg += f"  container: {container!r} (type: {type(container).__name__})\n"
        error_msg += f"  session: {session!r} (type: {type(session).__name__})"

        import traceback
        traceback.print_exc()
        raise ValueError(error_msg) from e



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

        self.session_dir = buffer_dir / container / f"session_{session}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir = archive_dir / container / f"session_{session}"
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        self.buffer_counter = self._get_last_buffer_number()
        self.buffer_size_limit = buffer_size_limit
        self.metadata_db = metadata_db
        self.current_writer: Optional[ipc.RecordBatchFileWriter] = None
        self.current_file: Optional[Path] = None
        self.current_sink: Optional[pa.NativeFile] = None
        self.current_size = 0
        self.lock = threading.Lock()

        # Load youngest archive back into buffer if it's small enough
        self._load_youngest_archive_if_small()

    def _get_last_buffer_number(self) -> int:
        """Track buffer counter in a small local file instead of scanning the directory."""
        counter_file = self.session_dir / "counter.txt"

        if not counter_file.exists():
            counter_file.write_text("0")
            return 0

        try:
            return int(counter_file.read_text().strip())
        except Exception:
            return 0

    def _get_next_buffer_path(self) -> Path:
        self.buffer_counter += 1
        (self.session_dir / "counter.txt").write_text(str(self.buffer_counter))
        return self.session_dir / f"buffer-{self.buffer_counter:04d}.arrow"

    def _load_youngest_archive_if_small(self):
        """
        Check if the youngest archive is small enough to be loaded back into the buffer.
        If so, load it back into the current buffer without deleting the archive or metadata.
        """
        youngest = self.metadata_db.get_youngest_archive(self.container, self.session)

        if youngest is None:
            return  # No archives yet

        # Check if the archive is small enough (less than half the buffer limit)
        if youngest['file_size'] >= self.buffer_size_limit / 2:
            return  # Archive is too large

        archive_path = Path(youngest['archive_path'])
        if not archive_path.exists():
            logger.warning(f"Archive file not found: {archive_path}")
            return

        try:
            # Read the Parquet file preserving Arrow types
            table = pq.read_table(archive_path)

            # Validate schema matches before loading
            if not table.schema.equals(LOG_SCHEMA):
                logger.warning(f"Archive schema mismatch. Expected {LOG_SCHEMA}, got {table.schema}. Skipping load.")
                return

            # Convert to Arrow IPC buffer
            self._init_new_buffer()
            assert self.current_writer is not None

            # Write all batches from the table (preserving Arrow types)
            for batch in table.to_batches():
                self.current_writer.write_batch(batch)
                self.current_size += batch.nbytes

            logger.info(f"Loaded youngest archive {archive_path.name} back into buffer "
                       f"({youngest['num_rows']} rows, {youngest['file_size']} bytes)")

        except Exception as e:
            logger.error(f"Error loading youngest archive: {e}")
            # If we failed, ensure we have a clean state
            if self.current_writer is not None:
                self.current_writer.close()
            if self.current_sink is not None:
                self.current_sink.close()
            self.current_writer = None
            self.current_sink = None
            self.current_file = None
            self.current_size = 0

    def append_batch(self, batch: pa.RecordBatch):
        """Append an Arrow RecordBatch to the buffer"""
        rotate_now = False
        buffer_file = None

        with self.lock:
            # Initialize writer if needed
            if self.current_writer is None:
                self._init_new_buffer()

            assert self.current_writer is not None

            # Write batch to buffer
            self.current_writer.write_batch(batch)

            # Track size using batch's memory size
            self.current_size += batch.nbytes

            # Determine if we need rotation
            if self.current_size >= self.buffer_size_limit:
                buffer_file = self._rotate_buffer_locked()
                rotate_now = True

        # Flush outside of lock for maximum concurrency
        if rotate_now:
            assert buffer_file is not None
            FLUSH_POOL.submit(self._process_buffer, buffer_file)

    def _rotate_buffer_locked(self) -> Path:
        """Rotate current writer to a new file. Must be called under lock."""
        if self.current_writer:
            self.current_writer.close()
        if self.current_sink:
            self.current_sink.close()

        old_file = self.current_file
        assert old_file is not None, "Cannot rotate buffer without an active file"

        # Prepare new buffer
        self.current_writer = None
        self.current_sink = None
        self.current_file = None
        self.current_size = 0

        self._init_new_buffer()

        return old_file

    def _init_new_buffer(self):
        """Initialize a new Arrow IPC buffer file using StreamWriter (faster for append-only)"""
        self.current_file = self._get_next_buffer_path()
        self.current_sink = pa.output_stream(str(self.current_file))
        self.current_writer = ipc.new_stream(self.current_sink, LOG_SCHEMA)
        self.current_size = 0

    def _flush_buffer(self, do_async: bool = True):
        """Flush current buffer: convert to Parquet and archive"""
        with self.lock:
            if self.current_writer is None:
                return
            buffer_file = self._rotate_buffer_locked()

        if do_async:
            FLUSH_POOL.submit(self._process_buffer, buffer_file)
        else:
            self._process_buffer(buffer_file)

    def _process_buffer(self, buffer_file: Path):
        """Async processing: Arrow -> Parquet -> Archive -> Delete"""
        try:
            # Prefer input_stream for smaller files (faster than mmap for <50MB)
            with pa.input_stream(str(buffer_file)) as source:
                reader = ipc.open_stream(source)
                table = reader.read_all()

            # Convert to Parquet with optimized settings
            parquet_file = buffer_file.with_suffix('.parquet')
            pq.write_table(
                table,
                parquet_file,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True,
                data_page_size=65536,
                dictionary_pagesize_limit=65536,
                coerce_timestamps='us'
            )

            # Move to archive directory
            archive_file = self.archive_dir / parquet_file.name
            parquet_file.rename(archive_file)

            # Insert metadata entry
            self._insert_metadata(
                str(archive_file), table.num_rows, archive_file.stat().st_size)

            # Delete local buffer file
            buffer_file.unlink(missing_ok=True)

            logger.info(f"Successfully flushed buffer {buffer_file.name} to archive: {archive_file}")

        except Exception as e:
            logger.error(f"Failure processing buffer {buffer_file}: {e}")

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
            logger.info(f"Metadata inserted for {archive_path}: {num_rows} rows, {file_size} bytes")
        except Exception as e:
            logger.error(f"Error inserting metadata: {e}")

    def close(self):
        """Close and flush current buffer"""
        with self.lock:
            if self.current_writer is not None:
                self._flush_buffer(do_async=False)