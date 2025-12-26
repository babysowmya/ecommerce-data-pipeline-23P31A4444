-- 1. Daily Sales Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    date_key INT PRIMARY KEY,
    total_transactions INT,
    total_revenue DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    unique_customers INT
);

-- 2. Product Performance Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    product_key INT PRIMARY KEY,
    total_quantity_sold INT,
    total_revenue DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    avg_discount_percentage DECIMAL(5,2)
);

-- 3. Customer Metrics Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_customer_metrics (
    customer_key INT PRIMARY KEY,
    total_transactions INT,
    total_spent DECIMAL(12,2),
    avg_order_value DECIMAL(12,2),
    last_purchase_date DATE
);