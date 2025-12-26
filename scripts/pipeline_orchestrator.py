import sys
import time
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

MAX_RETRIES = 3
BACKOFF = [1, 2, 4]

LOG_DIR = Path("logs")
DATA_DIR = Path("data/processed")
LOG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

RUN_ID = f"PIPE_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
LOG_FILE = LOG_DIR / f"pipeline_orchestrator_{RUN_ID}.log"

# Logging setup
error_handler = logging.FileHandler(LOG_DIR / "pipeline_errors.log")
error_handler.setLevel(logging.ERROR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        error_handler,
        logging.StreamHandler()
    ]
)

PYTHON_EXEC = sys.executable

PIPELINE_STEPS = [
    ("data_generation", [PYTHON_EXEC, "scripts/data_generation/generate_data.py"]),
    ("ingestion", [PYTHON_EXEC, "scripts/ingestion/ingest_to_staging.py"]),
    ("quality_checks", [PYTHON_EXEC, "scripts/quality_checks/validate_data.py"]),
    ("warehouse", [PYTHON_EXEC, "scripts/transformation/run_warehouse_etl.py"]),
    ("analytics", [PYTHON_EXEC, "scripts/analytics/generate_analytics.py"])
]

report = {
    "pipeline_execution_id": RUN_ID,
    "start_time": datetime.now(timezone.utc).isoformat(),
    "steps_executed": {},
    "data_quality_summary": {
        "quality_score": None,
        "critical_issues": 0
    },
    "errors": [],
    "warnings": []
}

def is_retryable_error(error_msg: str) -> bool:
    retryable_keywords = ["timeout", "connection", "temporarily unavailable"]
    return any(k in error_msg.lower() for k in retryable_keywords)

def run_step(step_name, command):
    for attempt in range(MAX_RETRIES):
        try:
            start = time.time()
            logging.info(f"Starting step: {step_name}")

            subprocess.run(command, check=True, timeout=600)

            duration = round(time.time() - start, 2)

            report["steps_executed"][step_name] = {
                "status": "success",
                "duration_seconds": duration,
                "records_processed": None,
                "retry_attempts": attempt
            }

            logging.info(f"Completed step: {step_name}")
            return True

        except subprocess.TimeoutExpired:
            logging.warning(f"Timeout in {step_name}, retrying...")

        except subprocess.CalledProcessError as e:
            logging.error(f"Error in {step_name}: {e}")
            if not is_retryable_error(str(e)):
                logging.error("Permanent error detected. Aborting retries.")
                break

        time.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])

    report["steps_executed"][step_name] = {
        "status": "failed",
        "error_message": f"{step_name} failed after retries",
        "retry_attempts": MAX_RETRIES
    }
    report["errors"].append(step_name)
    return False

def main():
    for step, cmd in PIPELINE_STEPS:
        if not run_step(step, cmd):
            report["status"] = "failed"
            break
    else:
        report["status"] = "success"

    report["end_time"] = datetime.now(timezone.utc).isoformat()
    report["total_duration_seconds"] = (
        datetime.fromisoformat(report["end_time"]) -
        datetime.fromisoformat(report["start_time"])
    ).total_seconds()

    with open(DATA_DIR / "pipeline_execution_report.json", "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Pipeline execution finished")

if __name__ == "__main__":
    main()