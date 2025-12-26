CREATE TABLE warehouse.dim_products(
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(50),
    price_range VARCHAR(20),           -- Budget, Mid-range, Premium
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);