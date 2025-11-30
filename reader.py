"""
Log Reader Module - Efficient log reading and filtering from Parquet archives and Arrow buffers
Optimized using pyarrow.dataset APIs for fast, parallel scanning and push-down filters

Key optimizations:
- Uses pyarrow.dataset for parallel scanning of Parquet files
- Push-down filters at dataset level for better performance
- Memory-mapped IPC files for efficient buffer reading
- Shared FilterUtils for DRY filter application
- Minimizes to_pylist() conversions until final output
- Uses Arrow compute functions for vectorized filtering
"""
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator, Tuple
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.compute as pc
import pyarrow.dataset as ds
import json
import logging

logger = logging.getLogger(__name__)


class FilterUtils:
    """Utilities to build dataset-pushdown filters and in-memory masks."""

    @staticmethod
    def to_dataset_filters(filters: Optional[List[Tuple]]) -> Optional[ds.Expression]:
        """
        Convert simple (col, op, val) filters into a pyarrow.dataset expression
        for push-down filtering at the dataset level.
        Supported ops: '>=', '<=', '==', '!=', '>', '<'
        """
        if not filters:
            return None

        expr: Optional[ds.Expression] = None
        for col, op, val in filters:
            field = ds.field(col)
            if op == '>=':
                e = field >= val
            elif op == '<=':
                e = field <= val
            elif op == '==':
                e = field == val
            elif op == '!=':
                e = field != val
            elif op == '>':
                e = field > val
            elif op == '<':
                e = field < val
            else:
                # Unknown operator — skip
                continue

            expr = e if expr is None else (expr & e)
        return expr

    @staticmethod
    def build_arrow_mask(batch: pa.RecordBatch, filters: Optional[List[Tuple]]) -> Optional[pa.Array]:
        """Build an Arrow boolean mask for a RecordBatch using pyarrow.compute."""
        if not filters:
            return None

        mask: Optional[pa.Array] = None
        for col, op, val in filters:
            arr = batch[col]
            if op == '>=':
                m = pc.greater_equal(arr, val)
            elif op == '<=':
                m = pc.less_equal(arr, val)
            elif op == '==':
                m = pc.equal(arr, val)
            elif op == '!=':
                m = pc.not_equal(arr, val)
            elif op == '>':
                m = pc.greater(arr, val)
            elif op == '<':
                m = pc.less(arr, val)
            else:
                continue

            mask = m if mask is None else pc.and_kleene(mask, m)
        return mask


class ArchiveReader:
    """Reader for archived Parquet log files using pyarrow.dataset for optimized scanning"""

    def __init__(self, archive_files: List[Dict[str, Any]]):
        """
        Initialize archive reader with list of Parquet files

        Args:
            archive_files: List of dicts with 'archive_path' and metadata
        """
        self.archive_files = archive_files
        # Build list of valid paths for dataset
        self.valid_paths = []
        for file_info in archive_files:
            path = Path(file_info['archive_path'])
            if path.exists():
                self.valid_paths.append(str(path))

    def _get_dataset(self) -> Optional[ds.Dataset]:
        """Create a dataset from valid archive paths"""
        if not self.valid_paths:
            return None
        try:
            return ds.dataset(self.valid_paths, format='parquet')
        except Exception as e:
            logger.warning(f"Could not create dataset: {e}")
            return None

    def read_all(self, filters: Optional[List[Tuple]] = None) -> List[Dict[str, Any]]:
        """
        Read all logs from archive files using dataset API

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            List of log entries as dictionaries
        """
        dataset = self._get_dataset()
        if not dataset:
            return []

        try:
            # Use dataset push-down filters for efficiency
            ds_filter = FilterUtils.to_dataset_filters(filters)
            table = dataset.to_table(filter=ds_filter)

            # Convert to list of dicts
            if table.num_rows > 0:
                rows = table.to_pylist()
                for row in rows:
                    if row.get('timestamp'):
                        row['timestamp'] = row['timestamp'].isoformat()
                return rows
            return []
        except Exception as e:
            logger.error(f"Error reading archive files: {e}")
            return []

    def stream(self, filters: Optional[List[Tuple]] = None, batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        Stream logs from archive files using dataset scanner

        Args:
            filters: Optional list of (column, operator, value) tuples
            batch_size: Number of rows to read per batch

        Yields:
            Individual log entries as dictionaries
        """
        dataset = self._get_dataset()
        if not dataset:
            return

        try:
            # Use dataset scanner with push-down filters
            ds_filter = FilterUtils.to_dataset_filters(filters)
            scanner = dataset.scanner(filter=ds_filter, batch_size=batch_size)

            # Stream batches
            for batch in scanner.to_batches():
                if batch.num_rows == 0:
                    continue

                rows = batch.to_pylist()
                for row in rows:
                    if row.get('timestamp'):
                        row['timestamp'] = row['timestamp'].isoformat()
                    yield row
        except Exception as e:
            logger.error(f"Error streaming archive files: {e}")

    def count(self, filters: Optional[List[Tuple]] = None) -> int:
        """
        Count logs in archive files using dataset

        Args:
            filters: Optional list of (column, operator, value) tuples

        Returns:
            Total number of matching log entries
        """
        dataset = self._get_dataset()
        if not dataset:
            return 0

        try:
            ds_filter = FilterUtils.to_dataset_filters(filters)
            scanner = dataset.scanner(filter=ds_filter)

            # Count rows efficiently
            total = 0
            for batch in scanner.to_batches():
                total += batch.num_rows
            return total
        except Exception as e:
            logger.error(f"Error counting archive files: {e}")
            return 0

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for archive files

        Returns:
            Dictionary with summary stats
        """
        files_scanned = len(self.valid_paths)
        total_rows = 0
        total_size = 0

        for path_str in self.valid_paths:
            path = Path(path_str)
            total_size += path.stat().st_size

        # Use dataset to count total rows efficiently
        dataset = self._get_dataset()
        if dataset:
            try:
                scanner = dataset.scanner()
                for batch in scanner.to_batches():
                    total_rows += batch.num_rows
            except Exception:
                pass

        return {
            'files_scanned': files_scanned,
            'total_rows': total_rows,
            'total_size_bytes': total_size
        }


class BufferReader:
    """Reader for active Arrow IPC buffer files with memory mapping"""

    def __init__(self, buffer_files: List[Path]):
        """
        Initialize buffer reader with list of Arrow IPC files

        Args:
            buffer_files: List of buffer file paths
        """
        self.buffer_files = buffer_files

    def read_all(self, filters: Optional[List[Tuple]] = None) -> List[Dict[str, Any]]:
        """
        Read all logs from buffer files using memory mapping

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
                # Read streaming IPC format (not file format)
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_stream(source)
                    table = reader.read_all()

                # Apply filters if needed using FilterUtils
                if filters and table.num_rows > 0:
                    filtered_batches = []
                    for batch in table.to_batches():
                        mask = FilterUtils.build_arrow_mask(batch, filters)
                        if mask is not None:
                            filtered_batch = batch.filter(mask)
                            if filtered_batch.num_rows > 0:
                                filtered_batches.append(filtered_batch)
                        else:
                            filtered_batches.append(batch)

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
                logger.warning(f"Could not read buffer file {buffer_path}: {e}")
                continue

        return all_logs

    def stream(self, filters: Optional[List[Tuple]] = None, batch_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        Stream logs from buffer files with memory mapping

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
                # Read streaming IPC format
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_stream(source)

                    # Stream format doesn't support random access by batch index
                    # Read all batches sequentially
                    table = reader.read_all()
                    
                    for batch in table.to_batches():
                        # Apply filters using FilterUtils
                        if filters:
                            mask = FilterUtils.build_arrow_mask(batch, filters)
                            if mask is not None:
                                batch = batch.filter(mask)

                        if batch.num_rows == 0:
                            continue

                        # Convert batch to Python objects
                        rows = batch.to_pylist()
                        for row in rows:
                            if row.get('timestamp'):
                                row['timestamp'] = row['timestamp'].isoformat()
                            yield row
            except Exception as e:
                logger.warning(f"Could not read buffer file {buffer_path}: {e}")
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
                    reader = ipc.open_stream(source)

                    # Read table and count rows
                    if filters:
                        table = reader.read_all()
                        for batch in table.to_batches():
                            mask = FilterUtils.build_arrow_mask(batch, filters)
                            if mask is not None:
                                filtered_batch = batch.filter(mask)
                                total += filtered_batch.num_rows
                            else:
                                total += batch.num_rows
                    else:
                        # Just count rows
                        table = reader.read_all()
                        total += table.num_rows
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

                # Read streaming IPC file metadata
                with pa.memory_map(str(buffer_path), 'r') as source:
                    reader = ipc.open_stream(source)
                    # Read all and count rows
                    table = reader.read_all()
                    total_rows += table.num_rows

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
        Apply filters to a RecordBatch using FilterUtils

        Args:
            batch: Arrow RecordBatch
            filters: List of (column, operator, value) tuples

        Returns:
            Filtered RecordBatch
        """
        mask = FilterUtils.build_arrow_mask(batch, filters)
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

    def with_filters(self, filters: List[Tuple]) -> 'LogReader':
        """
        Add custom filters

        Args:
            filters: List of (column, operator, value) tuples

        Returns:
            Self for chaining
        """
        self.filters.extend(filters)
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

        logger.debug(f"Retrieved {len(archive_logs)} logs from archive files.")

        all_logs.extend(archive_logs)

        buffer_logs = self.buffer_reader.read_all(self.filters if self.filters else None)

        logger.debug(f"Retrieved {len(buffer_logs)} logs from buffer files.")

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

        logger.debug(f"Found {len(archive_files)} archived files for container '{container}', session '{session}'.")

        # Get active buffer files from file system
        session_buffer_dir = buffer_dir / container / f"session_{session}"
        buffer_files = []
        if session_buffer_dir.exists():
            buffer_files = sorted(session_buffer_dir.glob("buffer-*.arrow"))

        logger.debug(f"Found {len(buffer_files)} buffer files for container '{container}', session '{session}'.")
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
