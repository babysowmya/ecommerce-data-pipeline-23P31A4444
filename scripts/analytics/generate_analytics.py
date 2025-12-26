# scripts/transformation/generate_analytics.py

import psycopg2
import pandas as pd
import json
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# PostgreSQL connection parameters
conn_params = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "postgres",
    "password": "Sowmyasunkara@123"
}

# Dictionary of queries
queries = {
    "query1_top_products": """
    SELECT p.product_name,
           p.category,
           SUM(f.line_total) AS total_revenue,
           SUM(f.quantity) AS units_sold,
           AVG(f.unit_price) AS avg_price
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p
      ON f.product_key = p.product_key
    GROUP BY p.product_name, p.category
    ORDER BY total_revenue DESC
    LIMIT 10;
    """,

    "query2_monthly_trend": """
    SELECT CONCAT(d.year, '-', LPAD(d.month::text,2,'0')) AS year_month,
           SUM(f.line_total) AS total_revenue,
           COUNT(DISTINCT f.transaction_id) AS total_transactions,
           ROUND(SUM(f.line_total)/COUNT(DISTINCT f.transaction_id),2) AS avg_order_value,
           COUNT(DISTINCT f.customer_key) AS unique_customers
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d
      ON f.date_key = d.date_key
    GROUP BY year_month
    ORDER BY year_month;
    """,

    "query3_customer_segmentation": """
    WITH customer_totals AS (
        SELECT f.customer_key,
               c.customer_id,
               c.full_name,
               SUM(f.line_total) AS total_spent,
               AVG(f.line_total) AS avg_transaction_value,
               COUNT(DISTINCT f.transaction_id) AS transaction_count
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_customers c
          ON f.customer_key = c.customer_key
        GROUP BY f.customer_key, c.customer_id, c.full_name
    )
    SELECT CASE 
               WHEN total_spent <= 1000 THEN '$0-$1,000'
               WHEN total_spent <= 5000 THEN '$1,000-$5,000'
               WHEN total_spent <= 10000 THEN '$5,000-$10,000'
               ELSE '$10,000+'
           END AS spending_segment,
           COUNT(*) AS customer_count,
           SUM(total_spent) AS total_revenue,
           ROUND(AVG(avg_transaction_value),2) AS avg_transaction_value
    FROM customer_totals
    GROUP BY spending_segment
    ORDER BY spending_segment;
    """,

    "query4_category_performance": """
    SELECT p.category,
          SUM(f.line_total) AS total_revenue,
          SUM(f.quantity) AS units_sold
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p
      ON f.product_key = p.product_key
    GROUP BY p.category
    ORDER BY total_revenue DESC;

    """,

    "query5_payment_distribution": """
    WITH total AS (
        SELECT COUNT(*) AS total_transactions,
               SUM(line_total) AS total_revenue
        FROM warehouse.fact_sales
    )
    SELECT pm.payment_method_name AS payment_method,
           COUNT(f.sales_key) AS transaction_count,
           SUM(f.line_total) AS total_revenue,
           ROUND(COUNT(f.sales_key)::decimal / t.total_transactions * 100,2) AS pct_of_transactions,
           ROUND(SUM(f.line_total)/t.total_revenue *100,2) AS pct_of_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_payment_method pm
      ON f.payment_method_key = pm.payment_method_key
    CROSS JOIN total t
    GROUP BY pm.payment_method_name, t.total_transactions, t.total_revenue
    ORDER BY transaction_count DESC;
    """,

    "query6_geographic_analysis": """
    SELECT c.state,
           SUM(f.line_total) AS total_revenue,
           COUNT(DISTINCT f.customer_key) AS total_customers,
           ROUND(SUM(f.line_total)/COUNT(DISTINCT f.customer_key),2) AS avg_revenue_per_customer
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c
      ON f.customer_key = c.customer_key
    GROUP BY c.state
    ORDER BY total_revenue DESC;
    """,

    "query7_customer_lifetime_value": """
    SELECT c.customer_id,
          c.full_name,
          SUM(f.line_total) AS total_spent,
          COUNT(DISTINCT f.transaction_id) AS transaction_count,
          (CURRENT_DATE - c.registration_date) AS days_since_registration,
          ROUND(SUM(f.line_total)/COUNT(DISTINCT f.transaction_id),2) AS avg_order_value
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c
      ON f.customer_key = c.customer_key
    GROUP BY c.customer_id, c.full_name, c.registration_date
    ORDER BY total_spent DESC;
    """,

    "query8_product_profitability": """
    SELECT p.product_name,
           p.category,
           SUM(f.line_total) AS revenue,
           SUM(f.quantity) AS units_sold
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p
      ON f.product_key = p.product_key
    GROUP BY p.product_name, p.category
    ORDER BY revenue DESC
    LIMIT 10;
    """,

    "query9_day_of_week_pattern": """
    SELECT d.day_name,
           ROUND(AVG(f.line_total),2) AS avg_daily_revenue,
           COUNT(f.sales_key) AS total_daily_transactions,
           SUM(f.line_total) AS total_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d
      ON f.date_key = d.date_key
    GROUP BY d.day_name
    ORDER BY CASE d.day_name
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;
    """,

    "query10_discount_impact": """
    WITH discount_calc AS (
    SELECT quantity,
           line_total,
           0 AS discount_percentage   -- fallback, since original_price missing
    FROM warehouse.fact_sales
    )
    SELECT CASE
           WHEN discount_percentage = 0 THEN '0%'
           WHEN discount_percentage <=10 THEN '1-10%'
           WHEN discount_percentage <=25 THEN '11-25%'
           WHEN discount_percentage <=50 THEN '26-50%'
           ELSE '50%+'
       END AS discount_range,
       ROUND(AVG(discount_percentage),2) AS avg_discount_pct,
       SUM(quantity) AS total_quantity_sold,
       SUM(line_total) AS total_revenue,
       ROUND(AVG(line_total),2) AS avg_line_total
    FROM discount_calc
    GROUP BY discount_range
    ORDER BY discount_range;
    """
}

# Execute query and return DataFrame
def execute_query(connection, sql):
    return pd.read_sql(sql, connection)

# Export DataFrame to CSV
def export_to_csv(df, filename):
    df.to_csv(f"data/processed/analytics/{filename}", index=False)

def main():
    results_summary = {}
    start_time = time.time()

    conn = psycopg2.connect(**conn_params)

    for name, sql in queries.items():
        q_start = time.time()
        df = execute_query(conn, sql)
        export_to_csv(df, f"{name}.csv")
        q_end = time.time()
        results_summary[name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": round((q_end - q_start) * 1000, 2)
        }
        print(f"{name} exported with {len(df)} rows.")

    conn.close()

    results_summary["generation_timestamp"] = datetime.now().isoformat()
    results_summary["queries_executed"] = len(queries)
    results_summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)

    with open("data/processed/analytics/analytics_summary.json", "w") as f:
        json.dump(results_summary, f, indent=4)

    print("Analytics summary generated.")

if __name__ == "__main__":
    main()