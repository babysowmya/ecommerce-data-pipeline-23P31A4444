import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]  # ETL_pipeline root
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATA_PATH = "data/raw"
SUMMARY_PATH = "data/staging/ingestion_summary.json"

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    port=os.getenv("DB_PORT")
)

tables = ["customers", "products", "transactions", "transaction_items"]

summary = {"ingestion_timestamp": datetime.now().isoformat(), "tables_loaded": {}}
start = time.time()

try:
    with conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"TRUNCATE staging.{table}")
                df = pd.read_csv(f"{DATA_PATH}/{table}.csv")
                df.to_sql(table, conn, schema="staging", if_exists="append", index=False)
                summary["tables_loaded"][f"staging.{table}"] = {
                    "rows_loaded": len(df),
                    "status": "success"
                }
except Exception as e:
    conn.rollback()
    summary["error"] = str(e)
finally:
    conn.close()

summary["total_execution_time_seconds"] = round(time.time() - start, 2)

with open(SUMMARY_PATH, "w") as f:
    json.dump(summary, f, indent=4)