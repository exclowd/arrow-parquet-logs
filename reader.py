"""
Log Reader Module - Efficient log reading and filtering from Parquet archives and Arrow buffers
"""
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator, Tuple
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pyarrow.compute as pc
import json


class ArchiveReader:
    """Reader for archived Parquet log files"""

    def __init__(self, archive_files: List[Dict[str, Any]]):
        """
        Initialize archive reader with list of Parquet files

        Args:
            archive_files: List of dicts with 'archive_path' and metadata
        """
        self.archive_files = archive_files

    def read_all(self, filters: Optional[List[Tuple]] = None) -> List[Dict[str, Any]]:
        """
        Read all logs from archive files

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            List of log entries as dictionaries
        """
        all_logs = []

        for file_info in self.archive_files:
            archive_path = Path(file_info['archive_path'])
            if not archive_path.exists():
                continue

            # Use Parquet push-down filters for efficiency
            if filters:
                table = pq.read_table(archive_path, filters=filters)
            else:
                table = pq.read_table(archive_path)

            # Convert to list of dicts with minimal copying
            if table.num_rows > 0:
                rows = table.to_pylist()
                for row in rows:
                    if row.get('timestamp'):
                        row['timestamp'] = row['timestamp'].isoformat()
                    all_logs.append(row)

        return all_logs

    def stream(self, filters: Optional[List[Tuple]] = None, batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        Stream logs from archive files

        Args:
            filters: Optional list of (column, operator, value) tuples
            batch_size: Number of rows to read per batch

        Yields:
            Individual log entries as dictionaries
        """
        for file_info in self.archive_files:
            archive_path = Path(file_info['archive_path'])
            if not archive_path.exists():
                continue

            # Open Parquet file for batch reading
            parquet_file = pq.ParquetFile(archive_path)

            # Iterate through batches for memory efficiency
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                # Apply filters if Parquet doesn't support them at read time
                if filters:
                    batch = self._apply_filters(batch, filters)

                if batch.num_rows == 0:
                    continue

                # Convert batch to Python objects
                rows = batch.to_pylist()
                for row in rows:
                    if row.get('timestamp'):
                        row['timestamp'] = row['timestamp'].isoformat()
                    yield row

    def count(self, filters: Optional[List[Tuple]] = None) -> int:
        """
        Count logs in archive files

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            Total number of matching log entries
        """
        total = 0

        for file_info in self.archive_files:
            archive_path = Path(file_info['archive_path'])
            if not archive_path.exists():
                continue

            # Read with filters to get accurate count
            if filters:
                table = pq.read_table(archive_path, filters=filters, columns=['timestamp'])
                total += table.num_rows
            else:
                # Just read metadata for count
                parquet_file = pq.ParquetFile(archive_path)
                total += parquet_file.metadata.num_rows

        return total

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for archive files

        Returns:
            Dictionary with summary stats
        """
        files_scanned = 0
        total_rows = 0
        total_size = 0

        for file_info in self.archive_files:
            archive_path = Path(file_info['archive_path'])
            if not archive_path.exists():
                continue

            files_scanned += 1

            # Use metadata for fast stats
            parquet_file = pq.ParquetFile(archive_path)
            total_rows += parquet_file.metadata.num_rows
            total_size += archive_path.stat().st_size

        return {
            'files_scanned': files_scanned,
            'total_rows': total_rows,
            'total_size_bytes': total_size
        }

    def _apply_filters(self, batch, filters: List[Tuple]):
        """
        Apply filters to a RecordBatch

        Args:
            batch: Arrow RecordBatch
            filters: List of (column, operator, value) tuples

        Returns:
            Filtered RecordBatch
        """
        mask = None

        for col, op, val in filters:
            if op == '>=':
                bmask = pc.greater_equal(batch[col], val)
            elif op == '<=':
                bmask = pc.less_equal(batch[col], val)
            elif op == '==':
                bmask = pc.equal(batch[col], val)
            elif op == '!=':
                bmask = pc.not_equal(batch[col], val)
            else:
                continue

            if mask is None:
                mask = bmask
            else:
                mask = pc.and_kleene(mask, bmask)

        if mask is not None:
            return batch.filter(mask)
        return batch


class BufferReader:
    """Reader for active Arrow IPC buffer files"""

    def __init__(self, buffer_files: List[Path]):
        """
        Initialize buffer reader with list of Arrow IPC files

        Args:
            buffer_files: List of buffer file paths
        """
        self.buffer_files = buffer_files

    def read_all(self, filters: Optional[List[Tuple]] = None) -> List[Dict[str, Any]]:
        """
        Read all logs from buffer files

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            List of log entries as dictionaries
        """
        all_logs = []

        for buffer_path in self.buffer_files:
            if not buffer_path.exists():
                continue

            try:
                # Read Arrow IPC file
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_file(source)
                    table = reader.read_all()

                # Apply filters if needed
                if filters and table.num_rows > 0:
                    # Convert table to batches for filtering
                    filtered_batches = []
                    for batch in table.to_batches():
                        filtered_batch = self._apply_filters(batch, filters)
                        if filtered_batch.num_rows > 0:
                            filtered_batches.append(filtered_batch)
                    if filtered_batches:
                        table = pa.Table.from_batches(filtered_batches)
                    else:
                        continue

                # Convert to list of dicts
                if table.num_rows > 0:
                    rows = table.to_pylist()
                    for row in rows:
                        if row.get('timestamp'):
                            row['timestamp'] = row['timestamp'].isoformat()
                        all_logs.append(row)
            except Exception as e:
                # Skip corrupted or incomplete buffer files
                print(f"Warning: Could not read buffer file {buffer_path}: {e}")
                continue

        return all_logs

    def stream(self, filters: Optional[List[Tuple]] = None, batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        Stream logs from buffer files

        Args:
            filters: Optional list of (column, operator, value) tuples
            batch_size: Not used for buffer files (they're already in batches)

        Yields:
            Individual log entries as dictionaries
        """
        for buffer_path in self.buffer_files:
            if not buffer_path.exists():
                continue

            try:
                # Read Arrow IPC file
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_file(source)

                    # Read and process in batches
                    for i in range(reader.num_record_batches):
                        batch = reader.get_batch(i)

                        # Apply filters if needed
                        if filters:
                            batch = self._apply_filters(batch, filters)

                        if batch.num_rows == 0:
                            continue

                        # Convert batch to Python objects
                        rows = batch.to_pylist()
                        for row in rows:
                            if row.get('timestamp'):
                                row['timestamp'] = row['timestamp'].isoformat()
                            yield row
            except Exception as e:
                # Skip corrupted or incomplete buffer files
                print(f"Warning: Could not read buffer file {buffer_path}: {e}")
                continue

    def count(self, filters: Optional[List[Tuple]] = None) -> int:
        """
        Count logs in buffer files

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            Total number of matching log entries
        """
        total = 0

        for buffer_path in self.buffer_files:
            if not buffer_path.exists():
                continue

            try:
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_file(source)

                    # If filters, need to read and apply them
                    if filters:
                        for i in range(reader.num_record_batches):
                            batch = reader.get_batch(i)
                            filtered_batch = self._apply_filters(batch, filters)
                            total += filtered_batch.num_rows
                    else:
                        # Just count rows
                        for i in range(reader.num_record_batches):
                            batch = reader.get_batch(i)
                            total += batch.num_rows
            except Exception:
                # Skip corrupted files
                continue

        return total

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for buffer files

        Returns:
            Dictionary with summary stats
        """
        files_scanned = 0
        total_rows = 0
        total_size = 0

        for buffer_path in self.buffer_files:
            if not buffer_path.exists():
                continue

            try:
                files_scanned += 1

                # Read IPC file metadata
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_file(source)
                    # Count rows in all batches
                    for i in range(reader.num_record_batches):
                        batch = reader.get_batch(i)
                        total_rows += batch.num_rows

                total_size += buffer_path.stat().st_size
            except Exception:
                # Skip corrupted files in summary
                files_scanned -= 1
                continue

        return {
            'files_scanned': files_scanned,
            'total_rows': total_rows,
            'total_size_bytes': total_size
        }

    def _apply_filters(self, batch, filters: List[Tuple]):
        """
        Apply filters to a RecordBatch

        Args:
            batch: Arrow RecordBatch
            filters: List of (column, operator, value) tuples

        Returns:
            Filtered RecordBatch
        """
        mask = None

        for col, op, val in filters:
            if op == '>=':
                bmask = pc.greater_equal(batch[col], val)
            elif op == '<=':
                bmask = pc.less_equal(batch[col], val)
            elif op == '==':
                bmask = pc.equal(batch[col], val)
            elif op == '!=':
                bmask = pc.not_equal(batch[col], val)
            else:
                continue

            if mask is None:
                mask = bmask
            else:
                mask = pc.and_kleene(mask, bmask)

        if mask is not None:
            return batch.filter(mask)
        return batch


class LogReader:
    """Unified log reader that combines archive and buffer readers"""

    def __init__(self, archive_reader: ArchiveReader, buffer_reader: BufferReader):
        """
        Initialize log reader with archive and buffer readers

        Args:
            archive_reader: ArchiveReader instance for Parquet files
            buffer_reader: BufferReader instance for Arrow IPC files
        """
        self.archive_reader = archive_reader
        self.buffer_reader = buffer_reader
        self.filters = []

    def with_time_range(self, start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None) -> 'LogReader':
        """
        Add timestamp filters

        Args:
            start_time: Start timestamp (inclusive)
            end_time: End timestamp (inclusive)

        Returns:
            Self for chaining
        """
        if start_time:
            self.filters.append(('timestamp', '>=', start_time))
        if end_time:
            self.filters.append(('timestamp', '<=', end_time))
        return self

    def read_all(self) -> List[Dict[str, Any]]:
        """
        Read all logs from both archive and buffer files

        Returns:
            List of log entries as dictionaries
        """
        all_logs = []

        # Read from archive files
        archive_logs = self.archive_reader.read_all(self.filters if self.filters else None)

        print(f"Debug: Retrieved {len(archive_logs)} logs from archive files.")

        all_logs.extend(archive_logs)

        buffer_logs = self.buffer_reader.read_all(self.filters if self.filters else None)

        print(f"Debug: Retrieved {len(buffer_logs)} logs from buffer files.")

        all_logs.extend(buffer_logs)

        return all_logs

    def stream(self, batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        Stream logs from both archive and buffer files

        Args:
            batch_size: Number of rows to read per batch

        Yields:
            Individual log entries as dictionaries
        """
        # Stream from archive files
        for log in self.archive_reader.stream(self.filters if self.filters else None, batch_size):
            yield log

        # Stream from buffer files
        for log in self.buffer_reader.stream(self.filters if self.filters else None, batch_size):
            yield log

    def stream_json(self, batch_size: int = 1000) -> Generator[str, None, None]:
        """
        Stream logs as JSON strings (for HTTP responses)

        Args:
            batch_size: Number of rows to read per batch

        Yields:
            JSON strings of log entries
        """
        first = True
        for log in self.stream(batch_size=batch_size):
            if not first:
                yield ','
            else:
                first = False
            yield json.dumps(log)

    def count(self) -> int:
        """
        Count total matching logs from both sources

        Returns:
            Total number of matching log entries
        """
        total = 0
        total += self.archive_reader.count(self.filters if self.filters else None)
        total += self.buffer_reader.count(self.filters if self.filters else None)
        return total

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics from both archive and buffer files

        Returns:
            Dictionary with summary stats
        """
        archive_summary = self.archive_reader.get_summary()
        buffer_summary = self.buffer_reader.get_summary()

        return {
            'files_scanned': archive_summary['files_scanned'] + buffer_summary['files_scanned'],
            'total_rows': archive_summary['total_rows'] + buffer_summary['total_rows'],
            'total_size_bytes': archive_summary['total_size_bytes'] + buffer_summary['total_size_bytes'],
            'has_filters': len(self.filters) > 0,
            'archive_files': archive_summary['files_scanned'],
            'buffer_files': buffer_summary['files_scanned']
        }


class LogReaderFactory:
    """Factory for creating LogReader instances"""

    @staticmethod
    def create_from_metadata(metadata_db, container: str, session: str, buffer_dir: Path) -> LogReader:
        """
        Create LogReader from metadata database and buffer directory
        Includes both archived Parquet files and active Arrow buffer files

        Args:
            metadata_db: MetadataDB instance
            container: Container name
            session: Session name
            buffer_dir: Path to buffer directory

        Returns:
            LogReader instance with both archive and buffer files
        """
        # Get archived files from metadata DB
        archive_files = metadata_db.get_session_files(container, session)

        print(f"Debug: Found {len(archive_files)} archived files for container '{container}', session '{session}'.")

        # Get active buffer files from file system
        session_buffer_dir = buffer_dir / container / f"session_{session}"
        buffer_files = []
        if session_buffer_dir.exists():
            buffer_files = sorted(session_buffer_dir.glob("buffer-*.arrow"))

        print(f"Debug: Found {buffer_files=} buffer files for container '{container}', session '{session}'.")
        archive_reader = ArchiveReader(archive_files)
        buffer_reader = BufferReader(buffer_files)

        return LogReader(archive_reader, buffer_reader)

    @staticmethod
    def create_from_paths(archive_paths: List[str], buffer_paths: Optional[List[str]] = None) -> LogReader:
        """
        Create LogReader from list of file paths

        Args:
            archive_paths: List of Parquet file paths
            buffer_paths: Optional list of Arrow IPC buffer file paths

        Returns:
            LogReader instance
        """
        archive_files = [{'archive_path': path} for path in archive_paths]
        buffer_files = [Path(path) for path in (buffer_paths or [])]

        # Create readers
        archive_reader = ArchiveReader(archive_files)
        buffer_reader = BufferReader(buffer_files)

        return LogReader(archive_reader, buffer_reader)
