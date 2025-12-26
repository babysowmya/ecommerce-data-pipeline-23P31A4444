import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "ecommerce_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cursor = conn.cursor()


# ---------------- DIM DATE ----------------
def load_dim_date(start_date="2023-01-01", end_date="2024-12-31"):
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    current = start
    rows = []

    while current <= end:
        rows.append((
            int(current.strftime("%Y%m%d")),
            current,
            current.year,
            (current.month - 1) // 3 + 1,
            current.month,
            current.day,
            current.strftime("%B"),
            current.strftime("%A"),
            current.isocalendar()[1],
            current.weekday() >= 5
        ))
        current += timedelta(days=1)

    cursor.execute("TRUNCATE warehouse.dim_date RESTART IDENTITY CASCADE;")
    cursor.executemany("""
        INSERT INTO warehouse.dim_date
        (date_key, full_date, year, quarter, month, day,
         month_name, day_name, week_of_year, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)

    conn.commit()
    print(f"Loaded {len(rows)} rows into warehouse.dim_date")


# ---------------- DIM PAYMENT METHOD ----------------
def load_dim_payment_method():
    data = [
        ("Credit Card", "Online"),
        ("Debit Card", "Online"),
        ("UPI", "Online"),
        ("Net Banking", "Online"),
        ("Cash on Delivery", "Offline")
    ]

    cursor.execute("TRUNCATE warehouse.dim_payment_method RESTART IDENTITY CASCADE;")
    cursor.executemany("""
        INSERT INTO warehouse.dim_payment_method
        (payment_method_name, payment_type)
        VALUES (%s,%s)
    """, data)

    conn.commit()
    print(f"Loaded {len(data)} rows into warehouse.dim_payment_method")


# ---------------- DIM CUSTOMERS (SCD TYPE 2) ----------------
def load_dim_customers(customers_df):
    for _, row in customers_df.iterrows():
        full_name = f"{row['first_name']} {row['last_name']}"

        cursor.execute("""
            SELECT customer_key, full_name, email, city, state, country, age_group
            FROM warehouse.dim_customers
            WHERE customer_id=%s AND is_current=TRUE
        """, (row["customer_id"],))

        existing = cursor.fetchone()

        if not existing:
            cursor.execute("""
                INSERT INTO warehouse.dim_customers
                (customer_id, full_name, email, city, state, country,
                 age_group, effective_date, is_current)
                VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,TRUE)
            """, (
                row["customer_id"], full_name, row["email"].lower(),
                row["city"], row["state"], row["country"], row["age_group"]
            ))
        else:
            if existing[1:] != (
                full_name, row["email"].lower(),
                row["city"], row["state"], row["country"], row["age_group"]
            ):
                cursor.execute("""
                    UPDATE warehouse.dim_customers
                    SET is_current=FALSE, end_date=CURRENT_DATE
                    WHERE customer_key=%s
                """, (existing[0],))

                cursor.execute("""
                    INSERT INTO warehouse.dim_customers
                    (customer_id, full_name, email, city, state, country,
                     age_group, effective_date, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,TRUE)
                """, (
                    row["customer_id"], full_name, row["email"].lower(),
                    row["city"], row["state"], row["country"], row["age_group"]
                ))

    conn.commit()
    print(f"Loaded {len(customers_df)} customers into warehouse.dim_customers")


# ---------------- DIM PRODUCTS (SCD TYPE 2 + PRICE RANGE) ----------------
def load_dim_products(products_df):
    for _, row in products_df.iterrows():
        price = float(row["price"])

        if price < 50:
            price_range = "Budget"
        elif price < 200:
            price_range = "Mid-range"
        else:
            price_range = "Premium"

        cursor.execute("""
            SELECT product_key, product_name, category, sub_category, brand, price, price_range
            FROM warehouse.dim_products
            WHERE product_id=%s AND is_current=TRUE
        """, (row["product_id"],))

        existing = cursor.fetchone()

        if not existing:
            cursor.execute("""
                INSERT INTO warehouse.dim_products
                (product_id, product_name, category, sub_category, brand,
                 price, price_range, effective_date, is_current)
                VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,TRUE)
            """, (
                row["product_id"], row["product_name"], row["category"],
                row["sub_category"], row["brand"], price, price_range
            ))
        else:
            if existing[1:] != (
                row["product_name"], row["category"],
                row["sub_category"], row["brand"], price, price_range
            ):
                cursor.execute("""
                    UPDATE warehouse.dim_products
                    SET is_current=FALSE, end_date=CURRENT_DATE
                    WHERE product_key=%s
                """, (existing[0],))

                cursor.execute("""
                    INSERT INTO warehouse.dim_products
                    (product_id, product_name, category, sub_category, brand,
                     price, price_range, effective_date, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,TRUE)
                """, (
                    row["product_id"], row["product_name"], row["category"],
                    row["sub_category"], row["brand"], price, price_range
                ))

    conn.commit()
    print(f"Loaded {len(products_df)} products into warehouse.dim_products")


# ---------------- FACT SALES ----------------
def load_fact_sales(transactions_df, transaction_items_df, products_df):
    txn_map = transactions_df.set_index("transaction_id").to_dict("index")

    cursor.execute("SELECT customer_id, customer_key FROM warehouse.dim_customers WHERE is_current=TRUE")
    customer_map = dict(cursor.fetchall())

    cursor.execute("SELECT product_id, product_key FROM warehouse.dim_products WHERE is_current=TRUE")
    product_map = dict(cursor.fetchall())

    cursor.execute("SELECT payment_method_name, payment_method_key FROM warehouse.dim_payment_method")
    payment_map = dict(cursor.fetchall())

    rows = []

    for _, item in transaction_items_df.iterrows():
        txn = txn_map[item["transaction_id"]]
        quantity = int(item["quantity"])
        unit_price = float(item["unit_price"])
        discount_pct = float(item["discount_percentage"])

        line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)
        discount_amount = round(quantity * unit_price * discount_pct / 100, 2)

        cost = float(products_df.loc[
            products_df["product_id"] == item["product_id"], "cost"
        ].values[0])

        profit = round(line_total - (cost * quantity), 2)

        rows.append((
            int(pd.to_datetime(txn["transaction_date"]).strftime("%Y%m%d")),
            customer_map[txn["customer_id"]],
            product_map[item["product_id"]],
            payment_map[txn["payment_method"]],
            item["transaction_id"],
            quantity,
            unit_price,
            discount_amount,
            line_total,
            profit,
            datetime.now()
        ))

    cursor.execute("TRUNCATE warehouse.fact_sales RESTART IDENTITY;")
    cursor.executemany("""
        INSERT INTO warehouse.fact_sales
        (date_key, customer_key, product_key, payment_method_key,
         transaction_id, quantity, unit_price, discount_amount,
         line_total, profit, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)

    conn.commit()
    print(f"Loaded {len(rows)} rows into warehouse.fact_sales")


# ---------------- AGGREGATES ----------------
def load_aggregates():
    cursor.execute("TRUNCATE warehouse.agg_daily_sales;")
    cursor.execute("TRUNCATE warehouse.agg_product_performance;")
    cursor.execute("TRUNCATE warehouse.agg_customer_metrics;")

    cursor.execute("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key;
    """)

    cursor.execute("""
        INSERT INTO warehouse.agg_product_performance
        SELECT
            product_key,
            SUM(quantity),
            SUM(line_total),
            SUM(profit),
            AVG(discount_amount * 100.0 / NULLIF(line_total,0))
        FROM warehouse.fact_sales
        GROUP BY product_key;
    """)

    cursor.execute("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT
            customer_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            AVG(line_total),
            MAX(created_at)
        FROM warehouse.fact_sales
        GROUP BY customer_key;
    """)

    conn.commit()
    print("Aggregates refreshed successfully")


def close_connection():
    cursor.close()
    conn.close()
    print("Database connection closed.")