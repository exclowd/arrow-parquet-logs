from functools import wraps
from flask import request, jsonify
from pathlib import Path
import sqlite3
import secrets
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

class AuthDB:
    """Authentication and authorization database"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """Initialize auth database tables"""
        # Users table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tokens table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tokens_user
            ON tokens (user_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tokens_expires
            ON tokens (expires_at)
        """)

        # Containers table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS containers (
                container_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_containers_user
            ON containers (user_id)
        """)

        # Sessions table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                container_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (container_id) REFERENCES containers(container_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sessions_container
            ON sessions (container_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sessions_user
            ON sessions (user_id)
        """)

        self.conn.commit()

        # Create default admin user if not exists
        self._create_default_admin()

    def _create_default_admin(self):
        """Create default admin user with password 'admin'"""
        cursor = self.conn.execute("SELECT user_id FROM users WHERE user_id = 'admin'")
        if cursor.fetchone() is None:
            password_hash = self._hash_password('admin')
            self.conn.execute(
                "INSERT INTO users (user_id, password_hash) VALUES (?, ?)",
                ('admin', password_hash)
            )
            self.conn.commit()
            print("Created default admin user (username: admin, password: admin)")

    def _hash_password(self, password: str) -> str:
        """Hash password with SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()

    def verify_password(self, user_id: str, password: str) -> bool:
        """Verify user password"""
        with self.lock:
            cursor = self.conn.execute(
                "SELECT password_hash FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return False

            password_hash = self._hash_password(password)
            return password_hash == row['password_hash']

    def create_token(self, user_id: str, expires_in_hours: int = 24) -> str:
        """Create new authentication token"""
        with self.lock:
            token = secrets.token_urlsafe(32)
            expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)

            self.conn.execute(
                "INSERT INTO tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
                (token, user_id, expires_at)
            )
            self.conn.commit()

            return token

    def verify_token(self, token: str) -> Optional[str]:
        """Verify token and return user_id if valid"""
        with self.lock:
            cursor = self.conn.execute("""
                SELECT user_id, expires_at FROM tokens
                WHERE token = ?
            """, (token,))

            row = cursor.fetchone()
            if row is None:
                return None

            # Check expiration
            expires_at = datetime.fromisoformat(row['expires_at'])
            if expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
                # Token expired, delete it
                self.conn.execute("DELETE FROM tokens WHERE token = ?", (token,))
                self.conn.commit()
                return None

            return row['user_id']

    def revoke_token(self, token: str):
        """Revoke a token"""
        with self.lock:
            self.conn.execute("DELETE FROM tokens WHERE token = ?", (token,))
            self.conn.commit()

    def cleanup_expired_tokens(self):
        """Remove expired tokens"""
        with self.lock:
            now = datetime.now(timezone.utc)
            self.conn.execute("DELETE FROM tokens WHERE expires_at < ?", (now,))
            self.conn.commit()

    def create_container(self, user_id: str, container_id: str) -> bool:
        """Create a new container for a user"""
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO containers (container_id, user_id) VALUES (?, ?)",
                    (container_id, user_id)
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def create_session(self, user_id: str, container_id: str, session_id: str) -> bool:
        """Create a new session for a container"""
        with self.lock:
            try:
                # Verify container belongs to user
                cursor = self.conn.execute(
                    "SELECT user_id FROM containers WHERE container_id = ?",
                    (container_id,)
                )
                row = cursor.fetchone()
                if row is None or row['user_id'] != user_id:
                    return False

                self.conn.execute(
                    "INSERT INTO sessions (session_id, container_id, user_id) VALUES (?, ?, ?)",
                    (session_id, container_id, user_id)
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def verify_container_access(self, user_id: str, container_id: str) -> bool:
        """Verify user has access to container"""
        with self.lock:
            cursor = self.conn.execute(
                "SELECT user_id FROM containers WHERE container_id = ?",
                (container_id,)
            )
            row = cursor.fetchone()
            return row is not None and row['user_id'] == user_id

    def verify_session_access(self, user_id: str, session_id: str) -> bool:
        """Verify user has access to session"""
        with self.lock:
            cursor = self.conn.execute(
                "SELECT user_id FROM sessions WHERE session_id = ?",
                (session_id,)
            )
            row = cursor.fetchone()
            return row is not None and row['user_id'] == user_id

    def get_user_containers(self, user_id: str) -> list:
        """Get all containers for a user"""
        with self.lock:
            cursor = self.conn.execute(
                "SELECT container_id, created_at FROM containers WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_container_sessions(self, user_id: str, container_id: str) -> list:
        """Get all sessions for a container"""
        with self.lock:
            cursor = self.conn.execute(
                "SELECT user_id FROM containers WHERE container_id = ?",
                (container_id,)
            )
            row = cursor.fetchone()
            if row is None or row['user_id'] != user_id:
                return []

            cursor = self.conn.execute(
                "SELECT session_id, created_at FROM sessions WHERE container_id = ? ORDER BY created_at DESC",
                (container_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Close database connection"""
        with self.lock:
            self.conn.close()


def require_authN(auth_db: AuthDB):
    """Decorator factory that takes AuthDB instance"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth_header = request.headers.get('Authorization')

            if not auth_header:
                return jsonify({'error': 'No authorization header provided'}), 401

            # Check if it's a Bearer token
            parts = auth_header.split()
            if len(parts) != 2 or parts[0].lower() != 'bearer':
                return jsonify({'error': 'Invalid authorization header format. Use: Bearer <token>'}), 401

            token = parts[1]

            # Verify token with database
            user_id = auth_db.verify_token(token)
            if user_id is None:
                return jsonify({'error': 'Invalid or expired token'}), 401

            # Pass user_id and token to the function
            return f(user_id, token, *args, **kwargs)

        return decorated_function
    return decorator