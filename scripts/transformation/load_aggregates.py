from scripts.utils.db import get_connection

conn = get_connection()
cursor = conn.cursor()

def load_aggregates():
    # Use global cursor and conn
    # 1. Daily sales aggregates
    cursor.execute("""
        INSERT INTO warehouse.agg_daily_sales (date_key, total_transactions, total_revenue, total_profit, unique_customers)
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key
        ON CONFLICT (date_key) DO UPDATE
        SET total_transactions = EXCLUDED.total_transactions,
            total_revenue = EXCLUDED.total_revenue,
            total_profit = EXCLUDED.total_profit,
            unique_customers = EXCLUDED.unique_customers;
    """)

    # 2. Product performance aggregates
    cursor.execute("""
        INSERT INTO warehouse.agg_product_performance (product_key, total_quantity_sold, total_revenue, total_profit, avg_discount_percentage)
        SELECT
            product_key,
            SUM(quantity),
            SUM(line_total),
            SUM(profit),
            AVG(discount_amount * 100.0 / NULLIF(line_total,0))
        FROM warehouse.fact_sales
        GROUP BY product_key
        ON CONFLICT (product_key) DO UPDATE
        SET total_quantity_sold = EXCLUDED.total_quantity_sold,
            total_revenue = EXCLUDED.total_revenue,
            total_profit = EXCLUDED.total_profit,
            avg_discount_percentage = EXCLUDED.avg_discount_percentage;
    """)

    # 3. Customer metrics aggregates
    cursor.execute("""
        INSERT INTO warehouse.agg_customer_metrics (customer_key, total_transactions, total_spent, avg_order_value, last_purchase_date)
        SELECT
            customer_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            AVG(line_total),
            MAX(created_at)
        FROM warehouse.fact_sales
        GROUP BY customer_key
        ON CONFLICT (customer_key) DO UPDATE
        SET total_transactions = EXCLUDED.total_transactions,
            total_spent = EXCLUDED.total_spent,
            avg_order_value = EXCLUDED.avg_order_value,
            last_purchase_date = EXCLUDED.last_purchase_date;
    """)

    conn.commit()