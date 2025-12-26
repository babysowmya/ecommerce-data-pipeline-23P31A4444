-- Data Freshness
SELECT MAX(created_at) AS latest_warehouse FROM warehouse.fact_sales;

-- Volume Trend (last 30 days)
SELECT date_key, COUNT(*) AS transactions
FROM warehouse.fact_sales
WHERE date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key;

-- Orphan Records
SELECT COUNT(*) FROM warehouse.fact_sales f
LEFT JOIN warehouse.dim_customers c
ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Execution History
SELECT * FROM pipeline_execution_log
ORDER BY start_time DESC
LIMIT 10;

-- Database Stats
SELECT relname, n_live_tup
FROM pg_stat_user_tables;