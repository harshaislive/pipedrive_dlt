import os
import sys
import threading
from flask import Flask, request, jsonify

# Import the run_sync function directly from our pipeline script
from pipedrive_pipeline import run_sync

# Ensure the project root is in the path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))

SYNC_LOCK = threading.Lock()
app = Flask(__name__)

# Use a consistent secret token from environment variables
SECRET_TOKEN = os.environ.get('WEBHOOK_SECRET_TOKEN', 'changeme')

def run_sync_in_background(full_sync: bool = False):
    """
    Helper function to run a sync process in a background thread.
    A "full sync" from the webhook's perspective will be a standard run,
    letting dlt manage state. True full syncs should be manual operations.
    """
    if not SYNC_LOCK.acquire(blocking=False):
        print("Sync already in progress. Request ignored.")
        return

    def task():
        sync_type = "Full" if full_sync else "Standard (Incremental)"
        try:
            print(f"--- Starting {sync_type} Sync in background ---")
            # The new run_sync handles incrementals by default.
            # We run with parallel=True for maximum speed.
            # The concept of a "full sync" is now a manual operation
            # that involves clearing the dlt state.
            run_sync(parallel=True)
            print(f"--- {sync_type} Sync finished ---")
        except Exception as e:
            print(f"--- ERROR during background {sync_type} Sync: {e} ---")
        finally:
            print(f"--- Releasing sync lock ({sync_type}) ---")
            SYNC_LOCK.release()

    thread = threading.Thread(target=task)
    thread.start()

@app.route('/incremental-sync', methods=['POST'])
def trigger_incremental_sync():
    """
    Endpoint to trigger a standard incremental sync. This is the primary
    endpoint for Pipedrive webhooks.
    """
    token = request.headers.get('X-Webhook-Token')
    if token != SECRET_TOKEN:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401

    if SYNC_LOCK.locked():
        return jsonify({'status': 'error', 'message': 'Sync already running'}), 409

    run_sync_in_background(full_sync=False)
    return jsonify({'status': 'started', 'message': 'Standard (incremental) sync started in background.'})

@app.route('/full-sync', methods=['POST'])
def trigger_full_sync():
    """
    Endpoint to trigger a one-off sync. Note: this does NOT perform a
    true "full sync" by clearing state, but rather a standard run.
    This is for manual triggering or compatibility.
    """
    token = request.headers.get('X-Webhook-Token')
    if token != SECRET_TOKEN:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401

    if SYNC_LOCK.locked():
        return jsonify({'status': 'error', 'message': 'Sync already running'}), 409

    run_sync_in_background(full_sync=True)
    return jsonify({'status': 'started', 'message': 'Standard sync (behaving as full on first run) started in background.'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting webhook and sync server on port {port}")
    app.run(host='0.0.0.0', port=port) 