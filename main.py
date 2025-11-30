from flask import Flask, jsonify, request, Response
from pathlib import Path
import signal
import sys
import json
from datetime import datetime
import traceback

from auth import require_authN, AuthDB
from metadata import MetadataDB
from writer import BufferManager, create_record_batch
from reader import LogReaderFactory

app = Flask(__name__)

# Configuration
BUFFER_DIR = Path("./tmp/buffers")
ARCHIVE_DIR = Path("./tmp/archives")
DB_DIR = Path("./tmp/db")
BUFFER_SIZE_LIMIT = 10 * 1024 * 1024  # 10 MB
METADATA_DB = Path(DB_DIR / "metadata.db")
AUTH_DB = Path(DB_DIR / "auth.db")

# Ensure directories exist
BUFFER_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DB.parent.mkdir(parents=True, exist_ok=True)

# Initialize databases
auth_db = AuthDB(AUTH_DB)
metadata_db = MetadataDB(METADATA_DB)

# Global buffer manager
buffer_manager = BufferManager(
    BUFFER_DIR,
    ARCHIVE_DIR,
    metadata_db,
    BUFFER_SIZE_LIMIT
)


@app.route('/')
def home():
    return jsonify({
        'message': 'Welcome to the Flask Log API',
        'status': 'running',
        'endpoints': {
            'auth': {
                'POST /api/auth/login': 'Login and get token',
                'POST /api/auth/logout': 'Logout and revoke token'
            },
            'containers': {
                'POST /api/containers': 'Create a new container',
                'GET /api/containers': 'List user containers'
            },
            'sessions': {
                'POST /api/containers/<container>/sessions': 'Create a new session',
                'GET /api/containers/<container>/sessions': 'List container sessions'
            },
            'logs': {
                'POST /api/logs/<container>/<session>': 'Write logs to session',
                'GET /api/logs/<container>/<session>': 'Read logs from session'
            }
        }
    })

# ============= AUTH ENDPOINTS =============


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login and receive authentication token"""
    try:
        data = request.get_json()

        if not data or 'username' not in data or 'password' not in data:
            return jsonify({'error': 'Missing username or password'}), 400

        username = data['username']
        password = data['password']

        # Verify credentials
        if not auth_db.verify_password(username, password):
            return jsonify({'error': 'Invalid credentials'}), 401

        # Create token
        token = auth_db.create_token(username, expires_in_hours=24)

        return jsonify({
            'token': token,
            'user_id': username,
            'expires_in_hours': 24
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/auth/logout', methods=['POST'])
@require_authN(auth_db)
def logout(user_id, token):
    """Logout and revoke token"""
    try:
        auth_db.revoke_token(token)
        return jsonify({'message': 'Logged out successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============= CONTAINER ENDPOINTS =============


@app.route('/api/containers', methods=['POST'])
@require_authN(auth_db)
def create_container(user_id, token):
    """Create a new container"""
    try:
        data = request.get_json()

        if not data or 'container_id' not in data:
            return jsonify({'error': 'Missing container_id'}), 400

        container_id = data['container_id']

        # Validate container_id format
        if not container_id or not container_id.replace('-', '').replace('_', '').isalnum():
            return jsonify({'error': 'Invalid container_id format. Use alphanumeric, hyphens, or underscores'}), 400

        # Create container
        if not auth_db.create_container(user_id, container_id):
            return jsonify({'error': 'Container already exists'}), 409

        return jsonify({
            'container_id': container_id,
            'user_id': user_id,
            'message': 'Container created successfully'
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/containers', methods=['GET'])
@require_authN(auth_db)
def list_containers(user_id, token):
    """List all containers for the user"""
    try:
        containers = auth_db.get_user_containers(user_id)
        return jsonify({
            'user_id': user_id,
            'containers': containers,
            'count': len(containers)
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============= SESSION ENDPOINTS =============


@app.route('/api/containers/<container_id>/sessions', methods=['POST'])
@require_authN(auth_db)
def create_session(user_id, token, container_id):
    """Create a new session in a container"""
    try:
        # Verify container access
        if not auth_db.verify_container_access(user_id, container_id):
            return jsonify({'error': 'Container not found or access denied'}), 403

        data = request.get_json()

        if not data or 'session_id' not in data:
            return jsonify({'error': 'Missing session_id'}), 400

        session_id = data['session_id']

        # Validate session_id format
        if not session_id or not session_id.replace('-', '').replace('_', '').isalnum():
            return jsonify({'error': 'Invalid session_id format. Use alphanumeric, hyphens, or underscores'}), 400

        full_session_id = f"{container_id}_{session_id}"

        # Create session with full session ID
        if not auth_db.create_session(user_id, container_id, full_session_id):
            return jsonify({'error': 'Session already exists or container not found'}), 409

        return jsonify({
            'session_id': session_id,
            'container_id': container_id,
            'user_id': user_id,
            'message': 'Session created successfully'
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/containers/<container_id>/sessions', methods=['GET'])
@require_authN(auth_db)
def list_sessions(user_id, token, container_id):
    """List all sessions in a container"""
    try:
        # Verify container access
        if not auth_db.verify_container_access(user_id, container_id):
            return jsonify({'error': 'Container not found or access denied'}), 403

        sessions = auth_db.get_container_sessions(user_id, container_id)

        formatted_sessions = []
        prefix = f"{container_id}_"
        for session in sessions:
            session_copy = session.copy()
            # Remove container prefix if present
            if session_copy['session_id'].startswith(prefix):
                session_copy['session_id'] = session_copy['session_id'][len(
                    prefix):]
            formatted_sessions.append(session_copy)

        return jsonify({
            'container_id': container_id,
            'user_id': user_id,
            'sessions': formatted_sessions,
            'count': len(formatted_sessions)
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============= LOG ENDPOINTS =============


@app.route('/api/logs/<container>/<session>', methods=['GET'])
@require_authN(auth_db)
def get_data(user_id, token, container, session):
    """Get logs for a session, optionally filtered by timestamp range"""
    try:
        # Verify session access
        if not auth_db.verify_session_access(user_id, f"{container}_{session}"):
            return jsonify({'error': 'Session not found or access denied'}), 403

        # Get query parameters
        start_ts = request.args.get('start_ts')
        end_ts = request.args.get('end_ts')
        stream = request.args.get('stream', 'false').lower() == 'true'

        # Create log reader using factory (includes both archive and buffer files)
        reader = LogReaderFactory.create_from_metadata(
            metadata_db, container, session, BUFFER_DIR)

        # Parse timestamps if provided
        start_time = None
        end_time = None
        if start_ts:
            start_time = datetime.fromisoformat(
                start_ts.replace('Z', '+00:00'))
        if end_ts:
            end_time = datetime.fromisoformat(end_ts.replace('Z', '+00:00'))

        # Apply time range filter
        reader.with_time_range(start_time, end_time)

        # Get summary for response metadata
        summary = reader.get_summary()

        # Handle empty results
        if summary['files_scanned'] == 0:
            return jsonify({
                'container': container,
                'session': session,
                'logs': [],
                'total_rows': 0,
                'files_scanned': 0
            })

        # Stream large result sets
        if stream:
            def generate():
                # Fix: Use json.dumps for proper escaping of container and session names
                yield '{"container":' + json.dumps(container) + ',"session":' + json.dumps(session) + ',"logs":['
                # Use LogReader's stream_json for efficient streaming
                for chunk in reader.stream_json(batch_size=1000):
                    yield chunk
                yield '],"files_scanned":' + str(summary['files_scanned']) + '}'

            return Response(generate(), mimetype='application/json')

        # Read all for smaller result sets
        else:
            logs = reader.read_all()

            return jsonify({
                'container': container,
                'session': session,
                'logs': logs,
                'total_rows': len(logs),
                'files_scanned': summary['files_scanned'],
                'filter': {
                    'start_ts': start_ts,
                    'end_ts': end_ts
                }
            })

    except ValueError as e:
        return jsonify({'error': f'Invalid timestamp format: {str(e)}'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/logs/<container>/<session>', methods=['POST'])
@require_authN(auth_db)
def post_data(user_id, token, container, session):
    """Append logs to the session buffer"""
    try:
        # Verify session access
        if not auth_db.verify_session_access(user_id, f"{container}_{session}"):
            return jsonify({'error': 'Session not found or access denied'}), 403

        data = request.get_json()

        # Validate request
        if not data or 'logs' not in data:
            return jsonify({'error': 'Missing "logs" field in request body'}), 400

        logs = data['logs']
        if not isinstance(logs, list):
            return jsonify({'error': '"logs" must be an array'}), 400

        if not logs:
            return jsonify({'error': '"logs" array cannot be empty'}), 400

        # Create and validate RecordBatch (validation happens in Arrow)
        # This is much faster than Python loop validation
        batch = create_record_batch(logs, container, session)

        # Get session buffer and append the batch
        buffer = buffer_manager.get_session_buffer(container, session)
        buffer.append_batch(batch)

        return jsonify({
            'container': container,
            'session': session,
            'message': 'Logs received',
            'count': len(logs)
        }), 201

    except ValueError as e:
        # Validation errors from create_record_batch
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(''.join(traceback.TracebackException.from_exception(e).format()))
        return jsonify({'error': str(e)}), 500


def cleanup_handler(signum=None, frame=None):
    """Gracefully shut down and flush all buffers"""
    print("\nShutting down gracefully...")
    try:
        buffer_manager.close_all()
        metadata_db.close()
        auth_db.close()
        print("All buffers flushed and databases closed.")
    except Exception as e:
        print(f"Error during cleanup: {e}")
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, cleanup_handler)
signal.signal(signal.SIGTERM, cleanup_handler)

if __name__ == "__main__":
    try:
        print("Starting Flask log server on port 5123...")
        print(f"Buffer directory: {BUFFER_DIR}")
        print(f"Archive directory: {ARCHIVE_DIR}")
        print(f"Metadata database: {METADATA_DB}")
        print(f"Auth database: {AUTH_DB}")
        print("\nDefault credentials:")
        print("  Username: admin")
        print("  Password: admin")
        print("\nPerformance optimizations:")
        print("  - Streaming GET responses (use ?stream=true)")
        # Fix: Disable reloader to prevent double initialization and signal handler conflicts
        app.run(debug=True, host='0.0.0.0', port=5123, use_reloader=False)
    finally:
        # Close all buffers on shutdown
        try:
            buffer_manager.close_all()
            metadata_db.close()
            auth_db.close()
        except Exception as e:
            print(f"Error closing resources: {e}")
