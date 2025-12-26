import schedule
import time
import subprocess
import logging
from pathlib import Path
import sys

LOCK_FILE = Path("pipeline.lock")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/scheduler_activity.log"),
        logging.StreamHandler()
    ]
)

def run_pipeline():
    if LOCK_FILE.exists():
        logging.warning("Pipeline already running. Skipping execution.")
        return

    try:
        LOCK_FILE.touch()
        logging.info("Starting scheduled pipeline run")

        # Use the current Python interpreter (venv) for subprocess
        python_executable = sys.executable

        subprocess.run([python_executable, "scripts/pipeline_orchestrator.py"], check=True)
        subprocess.run([python_executable, "scripts/cleanup_old_data.py"], check=True)

    except subprocess.CalledProcessError as e:
        logging.error(f"Pipeline execution failed: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        LOCK_FILE.unlink(missing_ok=True)
        logging.info("Pipeline and cleanup completed successfully")

# Run once immediately
run_pipeline()

# Optional: daily schedule
# schedule.every().day.at("02:00").do(run_pipeline)
# logging.info("Scheduler started and waiting for scheduled tasks")
# while True:
#     schedule.run_pending()
#     time.sleep(60)