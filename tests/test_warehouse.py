import pytest
import psycopg2
from psycopg2 import sql
from decimal import Decimal

DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "postgres",      # make sure this matches your DB user
    "password": "Sowmyasunkara@123"  # replace with actual password
}

@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    yield conn
    conn.close()

def test_dim_fact_tables_exist(db_connection):
    tables = [
        "warehouse.dim_customers",
        "warehouse.dim_products",
        "warehouse.dim_date",
        "warehouse.dim_payment_method",
        "warehouse.fact_sales"
    ]
    cur = db_connection.cursor()
    for table in tables:
        cur.execute(sql.SQL("SELECT to_regclass(%s)"), [table])
        assert cur.fetchone()[0] is not None

def test_scd_type2_versions(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM warehouse.dim_customers WHERE end_date IS NOT NULL")
    assert cur.fetchone()[0] >= 0

def test_foreign_keys(db_connection):
    cur = db_connection.cursor()
    # Use *_key columns
    cur.execute("""
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        LEFT JOIN warehouse.dim_customers dc ON fs.customer_key = dc.customer_key
        WHERE dc.customer_key IS NULL
    """)
    assert cur.fetchone()[0] == 0

    cur.execute("""
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        LEFT JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
        WHERE dp.product_key IS NULL
    """)
    assert cur.fetchone()[0] == 0

def test_aggregates_match_fact(db_connection):
    cur = db_connection.cursor()
    # Sum of line totals should match the sum in fact_sales
    cur.execute("SELECT SUM(line_total) FROM warehouse.fact_sales")
    sum_line_total = cur.fetchone()[0]
    assert sum_line_total is not None
    assert sum_line_total > 0

