# Log Server using Arrow IPC and Parquet

## Thesis:
If I can keep python out as much as possible, and just use it as a thin API layer.
Where the heavy lifting of log storage and querying is done by Arrow and Parquet,
then I can achieve high performance log streaming and querying with minimal overhead
and complexity.

## Overview

This API provides a high-performance log streaming service with:
- **Efficient Storage**: Arrow IPC buffers that auto-archive to Parquet
- **Time Range Queries**: Filter logs by start and end timestamps
- **Streaming Support**: Memory-efficient log reading for large datasets
- **Authentication**: Token-based authentication (24-hour expiry)
- **Access Control**: Users can only access their own containers and sessions

## Important Requirements

- **ISO 8601 string**: `"2025-11-30T12:00:00Z"`

Example:
```json
{
  "logs": [
    {"level": "INFO", "message": "User login", "timestamp": "2025-11-30T12:00:00Z"}
  ]
}
```

## Default Credentials

```
Username: admin
Password: admin
```

---

## API Endpoints

### 1. Login
Get an authentication token.

**Request:**
```bash
curl -X POST http://localhost:5123/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}'
```

**Response:**
```json
{
  "token": "abc123...",
  "user_id": "admin",
  "expires_in_hours": 24
}
```

---

### 2. Create Container
Create a new container to organize sessions.

**Request:**
```bash
curl -X POST http://localhost:5123/api/containers \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"container_id": "my-app-prod"}'
```

**Response:**
```json
{
  "container_id": "my-app-prod",
  "user_id": "admin",
  "message": "Container created successfully"
}
```

---

### 3. List Containers
Get all containers owned by the user.

**Request:**
```bash
curl -X GET http://localhost:5123/api/containers \
  -H "Authorization: Bearer ${TOKEN}"
```

**Response:**
```json
{
  "user_id": "admin",
  "containers": [
    {
      "container_id": "my-app-prod",
      "created_at": "2025-11-30 12:00:00"
    }
  ],
  "count": 1
}
```

---

### 4. Create Session
Create a new logging session within a container.

**Request:**
```bash
curl -X POST http://localhost:5123/api/containers/my-app-prod/sessions \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"session_id": "deployment-123"}'
```

**Response:**
```json
{
  "session_id": "deployment-123",
  "container_id": "my-app-prod",
  "user_id": "admin",
  "message": "Session created successfully"
}
```

---

### 5. List Sessions
Get all sessions in a container.

**Request:**
```bash
curl -X GET http://localhost:5123/api/containers/my-app-prod/sessions \
  -H "Authorization: Bearer ${TOKEN}"
```

**Response:**
```json
{
  "container_id": "my-app-prod",
  "user_id": "admin",
  "sessions": [
    {
      "session_id": "deployment-123",
      "created_at": "2025-11-30 12:05:00"
    }
  ],
  "count": 1
}
```

---

### 6. Write Logs
Append logs to a session (only if you own the session).

**Request:**
```bash
curl -X POST http://localhost:5123/api/logs/my-app-prod/deployment-123 \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "logs": [
      {"level": "INFO", "message": "Application started", "timestamp": "2025-11-30T11:00:00Z"},
      {"level": "ERROR", "message": "Connection failed", "timestamp": "2025-11-30T12:00:00Z"}
    ]
  }'
```

**Response:**
```json
{
  "container": "my-app-prod",
  "session": "deployment-123",
  "message": "Logs received",
  "count": 2
}
```

---

### 7. Read Logs
Read logs from a session (only if you own the session).

**Request:**
```bash
# Get all logs
curl -X GET http://localhost:5123/api/logs/my-app-prod/deployment-123 \
  -H "Authorization: Bearer ${TOKEN}"

# Get logs with time filter
curl -X GET "http://localhost:5123/api/logs/my-app-prod/deployment-123?start_ts=2025-11-30T10:00:00Z&end_ts=2025-11-30T12:00:00Z" \
  -H "Authorization: Bearer ${TOKEN}"

# Stream large results
curl -X GET "http://localhost:5123/api/logs/my-app-prod/deployment-123?stream=true" \
  -H "Authorization: Bearer ${TOKEN}"
```

**Response:**
```json
{
  "container": "my-app-prod",
  "session": "deployment-123",
  "logs": [
    {
      "timestamp": "2025-11-30T12:00:00+00:00",
      "level": "INFO",
      "message": "Application started",
      "container": "my-app-prod",
      "session": "deployment-123"
    }
  ],
  "total_rows": 1,
  "files_scanned": 1,
  "filter": {
    "start_ts": null,
    "end_ts": null
  }
}
```

---

### 8. Logout
Revoke the authentication token.

**Request:**
```bash
curl -X POST http://localhost:5123/api/auth/logout \
  -H "Authorization: Bearer ${TOKEN}"
```

**Response:**
```json
{
  "message": "Logged out successfully"
}
```

---

## Complete Workflow Example

```bash
# 1. Login
TOKEN=$(curl -s -X POST http://localhost:5123/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | jq -r '.token')

echo "Token: $TOKEN"

# 2. Create a container
curl -X POST http://localhost:5123/api/containers \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"container_id": "my-app"}'

# 3. Create a session
curl -X POST http://localhost:5123/api/containers/my-app/sessions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"session_id": "run-001"}'

# 4. Write logs
curl -X POST http://localhost:5123/api/logs/my-app/run-001 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "logs": [
      {"level": "INFO", "message": "Process started", "timestamp": "2025-11-30T10:00:00Z"},
      {"level": "INFO", "message": "Task completed", "timestamp": "2025-11-30T10:05:00Z"},
      {"level": "ERROR", "message": "Connection timeout", "timestamp": "2025-11-30T10:10:00Z"}
    ]
  }'

# 5. Read logs

curl -X GET http://localhost:5123/api/logs/my-app/run-001 \
  -H "Authorization: Bearer $TOKEN" | jq .

# 6. List all containers
curl -X GET http://localhost:5123/api/containers \
  -H "Authorization: Bearer $TOKEN" | jq .

# 7. List sessions in container
curl -X GET http://localhost:5123/api/containers/my-app/sessions \
  -H "Authorization: Bearer $TOKEN" | jq .

# 8. Logout
curl -X POST http://localhost:5123/api/auth/logout \
  -H "Authorization: Bearer $TOKEN"

```
---

## Database Schema

### Users Table
```sql
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Tokens Table
```sql
CREATE TABLE tokens (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

### Containers Table
```sql
CREATE TABLE containers (
    container_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

### Sessions Table
```sql
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    container_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (container_id) REFERENCES containers(container_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

---

## Testing Access Control

### Test 1: Unauthorized Access
```bash
# Try to access without token - should fail with 401
curl -X GET http://localhost:5123/api/logs/my-app/run-001
```

### Test 2: Create Container Without Login
```bash
# Try to create container without token - should fail with 401
curl -X POST http://localhost:5123/api/containers \
  -H "Content-Type: application/json" \
  -d '{"container_id": "test"}'
```

### Test 3: Access Another User's Session
If you had multiple users, trying to access another user's session would return 403.

---

## Notes

1. **Container IDs** must be alphanumeric with hyphens or underscores
2. **Session IDs** must be alphanumeric with hyphens or underscores
3. **Tokens** expire after 24 hours by default
4. **Passwords** are hashed with SHA-256 before storage
5. **Database** uses WAL mode for better concurrency
6. **All timestamps** are stored in UTC

---

## Production Recommendations

1. **Change default password** immediately
2. **Use HTTPS** in production
3. **Implement rate limiting** on login endpoint
4. **Add password complexity requirements**
5. **Consider JWT tokens** for stateless authentication
6. **Add user management endpoints** (create user, change password)
7. **Implement token refresh** mechanism
8. **Add audit logging** for access attempts
