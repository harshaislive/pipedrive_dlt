# Pipedrive to Postgres Sync with dlt - High-Performance Edition

This project syncs all important Pipedrive CRM data to a cloud PostgreSQL database using the [dlt](https://dlthub.com/) Python library. It has been optimized for high performance, featuring parallel data fetching and intelligent rate limiting.

## Key Features & Optimizations
- **High-Speed Sync**: Fetches data from the Pipedrive API concurrently, making it 5-10x faster than a standard sequential approach.
- **Intelligent Rate Limiting**: Automatically adjusts to Pipedrive's API rate limits with an exponential backoff strategy, maximizing throughput without causing errors.
- **Webhook Integration**: Designed to be triggered by Pipedrive webhooks for real-time, event-driven updates.
- **Efficient Incremental Syncs**: After the first run, `dlt` automatically tracks the last sync state and fetches only new or updated data.
- **Resilient and Robust**: Built-in retry mechanisms handle transient network or API errors gracefully.
- **Resource Granularity**: The sync process is broken into logical, parallelizable phases for efficiency and debugging.

## Setup

1.  **Navigate to the Pipeline Directory:**
    ```bash
    cd pipedrive_pipeline
    ```

2.  **Install Dependencies:**
    Make sure you have all the necessary packages installed.
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Credentials:**
    `dlt` handles secrets management. In the `pipedrive_pipeline` directory, you'll find a `.dlt` folder. Edit the `secrets.toml` file within it:
    ```toml
    # .dlt/secrets.toml
    [sources.pipedrive]
    pipedrive_api_key = "your_pipedrive_api_key_here" # required

    [destination.postgres]
    host = "your_postgres_host" # required
    database = "your_postgres_database" # required
    username = "your_postgres_username" # required
    password = "your_postgres_password" # required
    ```
    You also need to set a secret token for the webhook server. This can be done via an environment variable.

## Running the Pipeline

There are two primary ways to run the sync: remotely via an automation tool like n8n (recommended) or manually via the command line.

### 1. Automated Sync with n8n or Other Tools (Recommended)

The pipeline is designed to be triggered by any service that can send an HTTP POST request. This is the ideal way to integrate the sync into a larger workflow.

**Step 1: Start the Webhook Server**

First, the `webhook_server.py` must be running to listen for incoming requests.

1.  **Set the Webhook Secret Token:**
    This secret is used to authorize incoming requests.
    ```bash
    export WEBHOOK_SECRET_TOKEN="a_very_secure_secret_string"
    ```

2.  **Start the Server:**
    ```bash
    python webhook_server.py
    ```
    The server will start, typically on port 8080. If you are deploying this (e.g., on Railway, a VM, or a container service), make sure the server is running and the port is exposed.

**Step 2: Configure Your n8n Workflow**

In your n8n workflow, use the **HTTP Request** node to trigger the sync.

**Node Configuration:**
- **Method**: `POST`
- **URL**: `http://<your_server_address>:8080/incremental-sync`
  - Replace `<your_server_address>` with the IP or domain of where `webhook_server.py` is running.
- **Authentication**: `Header Auth`
- **Name**: `X-Webhook-Token`
- **Value**: The same secret token you set for `WEBHOOK_SECRET_TOKEN`.

Now, whenever this n8n node is executed, it will trigger a fast, parallel, incremental sync of your Pipedrive data.

### 2. Manual Sync via Command Line

You can run the pipeline directly for debugging, maintenance, or initial setup.

**Basic Command:**
This runs all phases in parallel and will perform an incremental update if a previous state exists.
```bash
python pipedrive_pipeline.py
```

**Command-Line Flags:**

-   `--sequential`: Runs the sync phases one by one. This is slower but can be useful for debugging.
    ```bash
    python pipedrive_pipeline.py --sequential
    ```
-   `--phase <number>`: Runs only a specific phase. This is useful if you only need to update a subset of your data.
    ```bash
    # Run only Phase 2 (Deals and related resources)
    python pipedrive_pipeline.py --phase 2
    ```

### Performing a True Full Sync

`dlt` automatically handles incremental loading. If you need to wipe the existing data and start from scratch, you must first clear the `dlt` state.

1.  **Drop the Destination Data (Optional but Recommended):**
    Manually delete the tables in your `pipedrive_data` schema in your PostgreSQL database.

2.  **Clear the dlt State:**
    Run the `dlt pipeline drop` command. This will erase the local state file that tracks previous syncs.
    ```bash
    dlt pipeline pipedrive drop
    ```

3.  **Run the Sync:**
    Now, run the pipeline manually. It will detect no prior state and perform a full load.
    ```bash
    python pipedrive_pipeline.py
    ```

## Monitoring the Pipeline

`dlt` provides powerful built-in tools for monitoring.

-   **Get Pipeline Info:**
    See a summary of the last run, including loaded tables and record counts.
    ```bash
    dlt pipeline pipedrive info
    ```

-   **Launch the Streamlit Dashboard:**
    For a visual overview of your data schemas, load history, and data tables.
    ```bash
    dlt pipeline pipedrive show
    ```

## Scheduling (Optional)

To run the sync every 6 hours, use cron, Airflow, or GitHub Actions. Example cron entry:
```
0 */6 * * * cd /path/to/pipedrive_pipeline && python pipedrive_pipeline.py
```

## References
- [dlt Pipedrive to Postgres Guide](https://dlthub.com/docs/pipelines/pipedrive/load-data-with-python-from-pipedrive-to-postgres)
- [dlt Incremental Loading](https://dlthub.com/docs/general-usage/incremental-loading) 