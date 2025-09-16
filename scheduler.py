import schedule
import time
import subprocess
import sys
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_pipeline():
    """Run the Pipedrive pipeline with incremental updates."""
    try:
        logging.info("Starting scheduled pipeline run")
        
        # Get the directory containing this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        pipeline_script = os.path.join(script_dir, "pipedrive_pipeline.py")
        
        # Run the pipeline with incremental updates
        result = subprocess.run(
            [sys.executable, pipeline_script],
            capture_output=True,
            text=True
        )
        
        # Log the output
        if result.stdout:
            logging.info("Pipeline output:\n%s", result.stdout)
        if result.stderr:
            logging.error("Pipeline errors:\n%s", result.stderr)
            
        # Check return code
        if result.returncode == 0:
            logging.info("Pipeline run completed successfully")
        else:
            logging.error("Pipeline run failed with return code %d", result.returncode)
            
    except Exception as e:
        logging.error("Error running pipeline: %s", str(e), exc_info=True)

def main():
    logging.info("Starting Pipedrive sync scheduler")
    
    # Schedule the job to run every 6 hours
    schedule.every(6).hours.do(run_pipeline)
    
    # Run immediately on start
    logging.info("Running initial sync...")
    run_pipeline()
    
    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute for pending jobs
        except Exception as e:
            logging.error("Scheduler error: %s", str(e), exc_info=True)
            time.sleep(300)  # On error, wait 5 minutes before retrying

if __name__ == "__main__":
    main() 