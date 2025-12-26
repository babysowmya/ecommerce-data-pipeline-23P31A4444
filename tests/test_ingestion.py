import psycopg2
import os
import pytest

@pytest.fixture
def db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    yield conn
    conn.rollback()
    conn.close()

def test_database_connection(db_connection):
    assert db_connection is not None

def test_staging_tables_exist(db_connection):
    tables = ["staging.customers", "staging.products", "staging.transactions", "staging.transaction_items"]
    cur = db_connection.cursor()
    for table in tables:
        cur.execute("SELECT to_regclass(%s)", (table,))
        assert cur.fetchone()[0] is not None

def test_staging_table_columns(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='staging' AND table_name='customers'")
    columns = [r[0] for r in cur.fetchall()]
    required_cols = ["customer_id", "first_name", "last_name", "email"]
    for col in required_cols:
        assert col in columns

def test_data_load_row_counts(db_connection):
    pass

def test_transaction_rollback(db_connection):
    cur = db_connection.cursor()
    try:
        cur.execute("INSERT INTO staging.customers(customer_id, first_name) VALUES('CUST9999','Test')")
        raise Exception("Force rollback")
    except:
        db_connection.rollback()
    cur.execute("SELECT * FROM staging.customers WHERE customer_id='CUST9999'")
    assert cur.fetchone() is None
