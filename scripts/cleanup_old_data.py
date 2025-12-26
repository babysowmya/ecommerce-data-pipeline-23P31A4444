import os
import logging
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIRS = ["data/raw", "data/staging", "logs"]
RETENTION_DAYS = 7

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/cleanup_activity.log"),
        logging.StreamHandler()
    ]
)

def cleanup():
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    for folder in DATA_DIRS:
        path = Path(folder)
        if not path.exists():
            continue
        for file in path.iterdir():
            if file.is_file():
                # Skip metadata, reports, current day's files
                if "metadata" in file.name.lower() or "report" in file.name.lower():
                    continue
                if file.stat().st_mtime < cutoff_date.timestamp():
                    try:
                        file.unlink()
                        logging.info(f"Deleted old file: {file}")
                    except Exception as e:
                        logging.error(f"Failed to delete {file}: {e}")

if __name__ == "__main__":
    logging.info("Starting cleanup process")
    cleanup()
    logging.info("Cleanup process completed")