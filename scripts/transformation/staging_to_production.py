import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------- CONNECTION HELPER ----------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "password"),
        port=os.getenv("DB_PORT", 5432)
    )

# ---------- CLEANING FUNCTIONS ----------
def clean_text(df, cols):
    for col in cols:
        df[col] = df[col].astype(str).str.strip().str.title()
    return df

def standardize_email(df):
    df['email'] = df['email'].str.lower()
    return df

def standardize_phone(df):
    df['phone'] = df['phone'].str.replace(r'\D','', regex=True)
    return df

# ---------- LOAD DIMENSIONS ----------
def load_customers(cur, conn):
    df = pd.read_sql("SELECT * FROM staging.customers", conn)
    df = clean_text(df, ['first_name','last_name','city','state','country'])
    df = standardize_email(df)
    df = standardize_phone(df)

    cur.execute("TRUNCATE production.customers")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO production.customers (customer_id, first_name, last_name, email, phone, registration_date)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (row['customer_id'], row['first_name'], row['last_name'], row['email'], row['phone'], row['registration_date']))
    conn.commit()

def load_products(cur, conn):
    df = pd.read_sql("SELECT * FROM staging.products", conn)
    df['profit_margin'] = round((df['price']-df['cost'])/df['price']*100,2)
    df['price_category'] = pd.cut(df['price'], bins=[0,50,200,10000], labels=['Budget','Mid-range','Premium'])
    df['product_name'] = df['product_name'].str.title()

    cur.execute("TRUNCATE production.products")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO production.products (product_id, product_name, category, sub_category, price, cost, brand, stock_quantity, supplier_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (row['product_id'], row['product_name'], row['category'], row['sub_category'],
              row['price'], row['cost'], row['brand'], row['stock_quantity'], row['supplier_id']))
    conn.commit()

# ---------- LOAD FACTS ----------
def load_transactions(cur, conn):
    df = pd.read_sql("SELECT * FROM staging.transactions", conn)
    cur.execute("SELECT transaction_id FROM production.transactions")
    existing = set([r[0] for r in cur.fetchall()])
    df = df[~df['transaction_id'].isin(existing)]

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO production.transactions (transaction_id, customer_id, transaction_date, transaction_time, payment_method, total_amount)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (row['transaction_id'], row['customer_id'], row['transaction_date'], row['transaction_time'], row['payment_method'], row['total_amount']))
    conn.commit()

def load_transaction_items(cur, conn):
    df = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
    cur.execute("SELECT item_id FROM production.transaction_items")
    existing = set([r[0] for r in cur.fetchall()])
    df = df[~df['item_id'].isin(existing)]

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO production.transaction_items (item_id, transaction_id, product_id, quantity, unit_price, discount_percentage, line_total)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (row['item_id'], row['transaction_id'], row['product_id'], row['quantity'],
              row['unit_price'], row['discount_percentage'], row['line_total']))
    conn.commit()

# ---------- MAIN FUNCTION ----------
def run_staging_to_production():
    conn = get_connection()
    cur = conn.cursor()
    try:
        load_customers(cur, conn)
        load_products(cur, conn)
        load_transactions(cur, conn)
        load_transaction_items(cur, conn)
        print("✅ Staging to Production ETL completed successfully.")
    finally:
        cur.close()
        conn.close()

# ---------- EXECUTION ----------
if __name__ == "__main__":
    run_staging_to_production()
