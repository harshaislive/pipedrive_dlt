import logging; logging.basicConfig(level=logging.INFO)
import dlt
import sys
import os
import requests
import traceback
import time
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the phases and their resources, grouped logically and efficiently
RESOURCE_PHASES = {
    1: {
        "name": "Core Entities",
        "description": "Core metadata and user/company information",
        "resources": ["users", "organizations", "persons", "pipelines", "stages", "products"],
        "dependencies": []  # No dependencies
    },
    2: {
        "name": "Deals and Dependent Resources",
        "description": "Deals, leads, and all associated data like participants and flow",
        "resources": ["deals", "deals_participants", "deals_flow", "leads"],
        "dependencies": [1]  # Depends on core entities
    },
    3: {
        "name": "Activities and Attachments",
        "description": "All activities, notes, and files",
        "resources": ["activities", "call_logs", "notes", "files"],
        "dependencies": [2]  # Depends on deals being loaded
    }
}

print("--- Pipeline script started ---")

# --- Fix for module import ---
# The 'pipedrive' module was initialized in the parent directory.
# This adds the parent directory to the Python path to allow the import.
print("Attempting to fix Python module path...")
try:
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    print(f"Project root '{project_root}' added to sys.path.")
    from pipedrive import pipedrive_source
    print("Successfully imported 'pipedrive_source'.")
except ImportError:
    print("\n--- FATAL ERROR ---")
    print("Could not import 'pipedrive_source'.")
    print("This is likely because the 'pipedrive' directory created by 'dlt init' is not in the correct location.")
    print("Please ensure the 'pipedrive' directory is in the same folder as the 'pipedrive_pipeline' directory and try again.")
    sys.exit(1)
except Exception as e:
    print(f"\n--- FATAL ERROR during import: {e} ---")
    sys.exit(1)


def get_api_key() -> str:
    """Get Pipedrive API key from DLT secrets."""
    print("Resolving Pipedrive API key from secrets...")
    try:
        api_key = dlt.secrets['pipedrive_api_key']
        print("API key resolved successfully.")
        return api_key
    except Exception as e:
        print(f"Failed to get API key: {e}")
        sys.exit(1)

def get_postgres_last_update(resource: str) -> Optional[str]:
    """Query Postgres for the latest update_time or modified timestamp for a resource table."""
    # Map resource to table and column
    table_map = {
        'deals': ('deals', 'update_time'),
        'leads': ('leads', 'update_time'),
        'persons': ('persons', 'update_time'),
        'organizations': ('organizations', 'update_time'),
        'activities': ('activities', 'update_time'),
        'notes': ('notes', 'add_time'),
        'files': ('files', 'add_time'),
        'call_logs': ('call_logs', 'add_time'),
        'pipelines': ('pipelines', 'add_time'),
        'stages': ('stages', 'add_time'),
        # Add more mappings as needed
    }
    if resource not in table_map:
        return None
    table, column = table_map[resource]
    try:
        # Use the standard DATABASE_URL environment variable for the connection
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()
        cur.execute(f"SELECT MAX({column}) FROM pipedrive_data.{table}")
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result and result[0]:
            return result[0].strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Could not get last update from Postgres for {resource}: {e}")
    return None

def get_last_sync_time(pipeline: dlt.Pipeline, resource: str) -> str:
    """Get the last successful sync time for a resource."""
    try:
        state = pipeline.state.get(f"last_sync_{resource}")
        if state:
            return state
    except:
        pass
    # Try Postgres for last update
    pg_last = get_postgres_last_update(resource)
    if pg_last:
        print(f"Using last update from Postgres for {resource}: {pg_last}")
        return pg_last
    # Default to full sync
    return "1970-01-01 00:00:00"

def update_last_sync_time(pipeline: dlt.Pipeline, resource: str):
    """Update the last successful sync time for a resource."""
    pipeline.state[f"last_sync_{resource}"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# --- Helper function for paginated API requests ---
def _get_paginated_data(resource_name: str, api_key: str, modified_since: str = None) -> List[Dict[str, Any]]:
    """
    Get paginated data from Pipedrive API with proper rate limiting and error handling.
    Supports incremental loading with modified_since parameter.
    """
    url = f"https://api.pipedrive.com/v1/{resource_name}?api_token={api_key}"
    params = {"start": 0, "limit": 100}
    
    # Add modified_since parameter for incremental loading if provided
    if modified_since:
        params["modified_since"] = modified_since
        print(f"Fetching {resource_name} modified since: {modified_since}")
    
    start = 0
    limit = 100
    page_count = 0
    retry_count = 0
    max_retries = 3
    
    while True:
        print(f"Fetching page {page_count} for '{resource_name}' (start={start}, limit={limit})")
        try:
            # Add delay between requests to avoid rate limits
            if page_count > 0:
                time.sleep(1)  # 1 second delay between pages
            
            params["start"] = start
            response = requests.get(url, params=params, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_count += 1
                wait_time = 60 if retry_count <= 1 else 120  # Wait longer on subsequent retries
                print(f"Rate limit hit for '{resource_name}'. Waiting {wait_time} seconds... (Retry {retry_count}/{max_retries})")
                time.sleep(wait_time)
                if retry_count >= max_retries:
                    print(f"Max retries ({max_retries}) reached for '{resource_name}'. Skipping remaining pages.")
                    break
                continue
            
            # Reset retry count on successful request
            retry_count = 0
            response.raise_for_status()
            
            data = response.json()
            if not data.get("success", True):
                error_msg = data.get("error", "Unknown error")
                print(f"Pipedrive API error for '{resource_name}': {error_msg}")
                break

        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request for '{resource_name}' failed: {e}")
            if retry_count >= max_retries:
                print(f"Max retries ({max_retries}) reached for '{resource_name}'. Skipping remaining pages.")
                break
            retry_count += 1
            time.sleep(30)  # Wait 30 seconds before retrying
            continue

        if not data.get("data"):
            print(f"No more data for '{resource_name}'.")
            break

        page_data_count = len(data["data"])
        print(f"Received {page_data_count} items for '{resource_name}' in page {page_count}.")
        yield data["data"]
        
        page_count += 1
        if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            print(f"API indicated no more pages for '{resource_name}'.")
            break
        start += limit

# --- Custom resource definitions with incremental loading ---
@dlt.resource(name="notes", write_disposition="merge", primary_key="id")
def notes_resource(api_key: str = dlt.config.value, modified_since: str = None):
    print("--- Starting 'notes' resource ---")
    yield from _get_paginated_data("notes", api_key, modified_since)
    print("--- Finished 'notes' resource ---")

@dlt.resource(name="call_logs", write_disposition="merge", primary_key="id")
def call_logs_resource(api_key: str = dlt.config.value, modified_since: str = None):
    print("--- Starting 'call_logs' resource ---")
    yield from _get_paginated_data("callLogs", api_key, modified_since)
    print("--- Finished 'call_logs' resource ---")

@dlt.resource(name="files", write_disposition="merge", primary_key="id")
def files_resource(api_key: str = dlt.config.value, modified_since: str = None):
    print("--- Starting 'files' resource ---")
    yield from _get_paginated_data("files", api_key, modified_since)
    print("--- Finished 'files' resource ---")

def run_phase(phase_num: int, pipeline: dlt.Pipeline):
    """Run a specific phase of the pipeline."""
    if phase_num not in RESOURCE_PHASES:
        print(f"Error: Invalid phase number {phase_num}")
        return

    phase_config = RESOURCE_PHASES[phase_num]
    resources = phase_config["resources"]
    print(f"\n=== Running Phase {phase_num}: {phase_config['name']} ===")
    print(f"Description: {phase_config['description']}")
    print(f"Resources: {resources}")

    try:
        # Source is already configured for incremental loading by default
        source = pipedrive_source().with_resources(*resources)
        
        # Run the pipeline for the selected resources
        info = pipeline.run(source)
        
        # Print the outcome
        print(f"\n--- Phase {phase_num} run info ---")
        print(info)
        print(f"--- End of Phase {phase_num} run ---")
        return True

    except Exception as e:
        print(f"\n--- ERROR in phase {phase_num} ---")
        print(f"Error: {str(e)}")
        print("\nTraceback:")
        print(traceback.format_exc())
        return False

def run_sync(phase: int = None, parallel: bool = True):
    """
    Run the complete sync process, with smart parallelization that respects dependencies.
    
    Args:
        phase: If provided, runs only that specific phase.
        parallel: If True, runs independent phases concurrently where possible.
    """
    print("\n--- run_sync() started ---")
    
    # Initialize a pipeline
    pipeline = dlt.pipeline(
        pipeline_name='pipedrive',
        destination='postgres',
        dataset_name='pipedrive_data'
    )

    if phase:
        # Run a single specific phase
        print(f"Running in single-phase mode for phase: {phase}")
        run_phase(phase, pipeline)
    else:
        # Run all phases with dependency awareness
        phases_to_run = sorted(RESOURCE_PHASES.keys())
        if parallel:
            print("Running phases with smart parallelization...")
            
            # Track completed phases
            completed_phases = set()
            failed_phases = set()
            
            while len(completed_phases) + len(failed_phases) < len(phases_to_run):
                # Find phases that can run (all dependencies satisfied)
                runnable_phases = [
                    phase_num for phase_num in phases_to_run
                    if phase_num not in completed_phases
                    and phase_num not in failed_phases
                    and all(dep in completed_phases for dep in RESOURCE_PHASES[phase_num]["dependencies"])
                ]
                
                if not runnable_phases:
                    if failed_phases:
                        print("\nCannot proceed: Some required phases failed.")
                        break
                    else:
                        print("\nNo phases ready to run. This shouldn't happen!")
                        break
                
                print(f"\nExecuting phase(s) {runnable_phases} in parallel...")
                with ThreadPoolExecutor(max_workers=len(runnable_phases)) as executor:
                    future_to_phase = {
                        executor.submit(run_phase, phase_num, pipeline): phase_num 
                        for phase_num in runnable_phases
                    }
                    
                    for future in as_completed(future_to_phase):
                        phase_num = future_to_phase[future]
                        try:
                            success = future.result()
                            if success:
                                completed_phases.add(phase_num)
                                print(f"Phase {phase_num} completed successfully.")
                            else:
                                failed_phases.add(phase_num)
                                print(f"Phase {phase_num} failed.")
                        except Exception as exc:
                            failed_phases.add(phase_num)
                            print(f"Phase {phase_num} generated an exception: {exc}")
            
            if failed_phases:
                print(f"\nSync completed with errors. Failed phases: {failed_phases}")
            else:
                print("\nAll phases completed successfully!")
        else:
            print("Running all phases sequentially...")
            for phase_num in phases_to_run:
                # Check dependencies
                if any(dep not in completed_phases for dep in RESOURCE_PHASES[phase_num]["dependencies"]):
                    print(f"Skipping phase {phase_num} due to failed dependencies")
                    continue
                    
                try:
                    success = run_phase(phase_num, pipeline)
                    if success:
                        completed_phases.add(phase_num)
                    else:
                        print(f"Phase {phase_num} failed! Stopping sequential pipeline run.")
                        return
                except Exception:
                    print(f"Phase {phase_num} failed with an exception! Stopping pipeline.")
                    return
            print("\nAll sequential phases completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Pipedrive to PostgreSQL pipeline")
    parser.add_argument("--phase", type=int, help="Run a specific phase number")
    parser.add_argument("--sequential", action="store_true", help="Run phases sequentially instead of in parallel")
    args = parser.parse_args()

    run_sync(phase=args.phase, parallel=not args.sequential)