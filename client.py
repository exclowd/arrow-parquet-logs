#!/usr/bin/env python3
"""
Client for the Log Server API

This module provides a convenient Python interface for interacting with the log server.
It handles authentication, container/session management, and log streaming.

Example usage:
    from client import LogClient
    from datetime import datetime

    # Create client and login
    client = LogClient("http://localhost:5123")
    client.login("admin", "admin")

    # Create container and session
    client.create_container("my-app")
    client.create_session("my-app", "run-001")

    # Write logs
    logs = [
        {"level": "INFO", "message": "Process started", "timestamp": datetime.now().isoformat() + "Z"},
        {"level": "ERROR", "message": "Connection failed", "timestamp": datetime.now().isoformat() + "Z"}
    ]
    client.write_logs("my-app", "run-001", logs)

    # Read logs
    logs = client.read_logs("my-app", "run-001")
    print(logs)

    # Logout
    client.logout()
"""

import requests
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
import time
import threading
import random


class LogClientError(Exception):
    """Base exception for log client errors"""
    pass


class AuthenticationError(LogClientError):
    """Raised when authentication fails"""
    pass


class LogClient:
    """Client for interacting with the log server API"""

    def __init__(self, base_url: str = "http://localhost:5123"):
        """
        Initialize the log client

        Args:
            base_url: Base URL of the log server (default: http://localhost:5123)
        """
        self.base_url = base_url.rstrip('/')
        self.token: Optional[str] = None
        self.user_id: Optional[str] = None

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with authentication token"""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _handle_response(self, response: requests.Response) -> Any:
        """Handle API response and raise appropriate errors"""
        try:
            data = response.json()
        except ValueError:
            data = {"error": response.text}

        if response.status_code == 401:
            raise AuthenticationError(
                data.get("error", "Authentication failed"))
        elif response.status_code >= 400:
            raise LogClientError(
                f"API error ({response.status_code}): {data.get('error', 'Unknown error')}")

        return data

    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        Login and receive authentication token

        Args:
            username: Username for authentication
            password: Password for authentication

        Returns:
            dict: Response containing token, user_id, and expires_in_hours

        Raises:
            AuthenticationError: If credentials are invalid
            LogClientError: If request fails
        """
        url = f"{self.base_url}/api/auth/login"
        data = {"username": username, "password": password}

        response = requests.post(url, json=data)
        result = self._handle_response(response)

        self.token = result.get("token")
        self.user_id = result.get("user_id")

        return result

    def logout(self) -> Dict[str, Any]:
        """
        Logout and revoke token

        Returns:
            dict: Response message

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/auth/logout"
        response = requests.post(url, headers=self._get_headers())
        result = self._handle_response(response)

        self.token = None
        self.user_id = None

        return result

    def create_container(self, container_id: str) -> Dict[str, Any]:
        """
        Create a new container

        Args:
            container_id: ID for the new container (alphanumeric, hyphens, underscores)

        Returns:
            dict: Response with container details

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails or container already exists
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/containers"
        data = {"container_id": container_id}

        response = requests.post(url, json=data, headers=self._get_headers())
        return self._handle_response(response)

    def list_containers(self) -> Dict[str, Any]:
        """
        List all containers owned by the user

        Returns:
            dict: Response containing list of containers and count

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/containers"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)

    def create_session(self, container_id: str, session_id: str) -> Dict[str, Any]:
        """
        Create a new session within a container

        Args:
            container_id: ID of the container
            session_id: ID for the new session (alphanumeric, hyphens, underscores)

        Returns:
            dict: Response with session details

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails or session already exists
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/containers/{container_id}/sessions"
        data = {"session_id": session_id}

        response = requests.post(url, json=data, headers=self._get_headers())
        return self._handle_response(response)

    def list_sessions(self, container_id: str) -> Dict[str, Any]:
        """
        List all sessions in a container

        Args:
            container_id: ID of the container

        Returns:
            dict: Response containing list of sessions and count

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/containers/{container_id}/sessions"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)

    def write_logs(self, container_id: str, session_id: str, logs: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Write logs to a session

        Args:
            container_id: ID of the container
            session_id: ID of the session
            logs: List of log entries, each with 'level', 'message', and 'timestamp' (ISO 8601)

        Returns:
            dict: Response with confirmation and count

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails or validation errors

        Example:
            logs = [
                {"level": "INFO", "message": "Started", "timestamp": "2025-11-30T12:00:00Z"},
                {"level": "ERROR", "message": "Failed", "timestamp": "2025-11-30T12:01:00Z"}
            ]
            client.write_logs("my-app", "run-001", logs)
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/logs/{container_id}/{session_id}"
        data = {"logs": logs}

        response = requests.post(url, json=data, headers=self._get_headers())
        return self._handle_response(response)

    def read_logs(self,
                  container_id: str,
                  session_id: str,
                  start_ts: Optional[str] = None,
                  end_ts: Optional[str] = None,
                  stream: bool = False) -> Dict[str, Any]:
        """
        Read logs from a session

        Args:
            container_id: ID of the container
            session_id: ID of the session
            start_ts: Optional start timestamp (ISO 8601 format)
            end_ts: Optional end timestamp (ISO 8601 format)
            stream: Whether to use streaming mode for large results

        Returns:
            dict: Response containing logs and metadata

        Raises:
            AuthenticationError: If not logged in
            LogClientError: If request fails

        Example:
            # Get all logs
            logs = client.read_logs("my-app", "run-001")

            # Get logs with time filter
            logs = client.read_logs("my-app", "run-001",
                                   start_ts="2025-11-30T10:00:00Z",
                                   end_ts="2025-11-30T12:00:00Z")
        """
        if not self.token:
            raise AuthenticationError("Not logged in")

        url = f"{self.base_url}/api/logs/{container_id}/{session_id}"
        params = {}

        if start_ts:
            params["start_ts"] = start_ts
        if end_ts:
            params["end_ts"] = end_ts
        if stream:
            params["stream"] = "true"

        response = requests.get(
            url, params=params, headers=self._get_headers())
        return self._handle_response(response)


def threaded_demo():
    """
    Threaded demonstration with concurrent writer and reader.

    Writer thread: Writes logs every second
    Reader thread: Queries last 10 seconds of logs every second

    Runs until Ctrl+C is pressed.
    """
    print("=== Threaded Log Client Demo ===\n")

    # Shared state
    container_id = "threaded-app"
    session_id = f"session-{1}"
    stop_event = threading.Event()
    log_counter = [0]  # Use list for mutable shared counter

    # Setup: Login and create container/session
    print("Setting up...")
    client = LogClient("http://localhost:5123")

    try:
        client.login("admin", "admin")
        print(f"✓ Logged in as {client.user_id}")

        try:
            client.create_container(container_id)
            print(f"✓ Created container: {container_id}")
        except LogClientError as e:
            if "already exists" in str(e):
                print(f"✓ Using existing container: {container_id}")
            else:
                raise

        try:
            client.create_session(container_id, session_id)
            print(f"✓ Created session: {session_id}")
        except LogClientError as e:
            if "already exists" in str(e):
                print(f"✓ Using existing session: {session_id}")
            else:
                raise

        print("\nStarting writer and reader threads...\n")

        def writer_thread():
            """Write logs every second"""
            log_levels = ["INFO", "WARN", "ERROR", "DEBUG"]
            messages = [
                "Processing request",
                "Database query executed",
                "Cache hit",
                "API call completed",
                "Memory usage normal",
                "Connection established",
                "Task scheduled",
                "File uploaded"
            ]

            while not stop_event.is_set():
                try:
                    current_time = datetime.now(timezone.utc)
                    logs = [
                        {
                            "level": random.choice(log_levels),
                            "message": f"{random.choice(messages)} #{log_counter[0]}",
                            "timestamp": current_time.isoformat().replace('+00:00', 'Z')
                        }
                    ]

                    result = client.write_logs(container_id, session_id, logs)
                    log_counter[0] += 1
                    print(
                        f"[WRITER] Wrote {result['count']} log(s) - Total: {log_counter[0]}")

                except Exception as e:
                    print(f"[WRITER] Error: {e}")

                # Wait 1 second or until stop event
                stop_event.wait(1.0)

        def reader_thread():
            """Query last 10 seconds of logs every second"""
            # Give writer a moment to write some logs first
            time.sleep(2)

            while not stop_event.is_set():
                try:
                    # Calculate time range: last 10 seconds
                    end_time = datetime.now(timezone.utc)
                    start_time = end_time - timedelta(seconds=10)


                    result = client.read_logs(
                        container_id,
                        session_id,
                    )

                    total_rows = result.get('total_rows', 0)
                    print(
                        f"[READER] Retrieved {total_rows} log(s) from last 10 seconds")

                except Exception as e:
                    print(f"[READER] Error: {e}")

                # Wait 1 second or until stop event
                stop_event.wait(1.0)

        # Create and start threads
        writer = threading.Thread(target=writer_thread, name="WriterThread")
        reader = threading.Thread(target=reader_thread, name="ReaderThread")

        writer.daemon = True
        reader.daemon = True

        writer.start()
        reader.start()

        # Run until Ctrl+C
        try:
            print("Running... (Press Ctrl+C to stop)\n")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nStopping due to user interrupt...")

        # Stop threads
        print("\nStopping threads...")
        stop_event.set()

        writer.join(timeout=2)
        reader.join(timeout=2)

        print(f"\n✓ Threads stopped")
        print(f"✓ Total logs written: {log_counter[0]}")

        # Cleanup
        print("\nLogging out...")
        client.logout()
        print("✓ Logged out successfully")

        print("\n=== Threaded Demo completed successfully! ===")

    except AuthenticationError as e:
        print(f"❌ Authentication error: {e}")
    except LogClientError as e:
        print(f"❌ Client error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    threaded_demo()
