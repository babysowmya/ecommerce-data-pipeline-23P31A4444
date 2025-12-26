CREATE TABLE warehouse.fact_sales(
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES warehouse.dim_date(date_key),
    customer_key INT REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    payment_method_key INT REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),          -- Degenerate dimension
    quantity INT,
    unit_price DECIMAL(12,2),
    discount_amount DECIMAL(12,2),       -- Calculated
    line_total DECIMAL(12,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP
);