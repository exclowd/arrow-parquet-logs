
from pathlib import Path
import threading
import sqlite3

class MetadataDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = threading.Lock()
        # Fix: Use persistent connection with WAL mode
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_db()

    def _init_db(self):
        """Initialize the metadata database"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS log_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container TEXT NOT NULL,
                session TEXT NOT NULL,
                archive_path TEXT NOT NULL,
                num_rows INTEGER NOT NULL,
                file_size INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_container_session
            ON log_files (container, session)
        """)
        self.conn.commit()

    def insert_file_metadata(self, container: str, session: str, archive_path: str,
                            num_rows: int, file_size: int):
        """Insert metadata for a flushed log file"""
        with self.lock:
            self.conn.execute("""
                INSERT INTO log_files (container, session, archive_path, num_rows, file_size)
                VALUES (?, ?, ?, ?, ?)
            """, (container, session, archive_path, num_rows, file_size))
            self.conn.commit()

    def get_session_files(self, container: str, session: str):
        """Get all archived files for a session"""
        with self.lock:
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.execute("""
                SELECT * FROM log_files
                WHERE container = ? AND session = ?
                ORDER BY created_at DESC
            """, (container, session))
            return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Close database connection"""
        with self.lock:
            self.conn.close()