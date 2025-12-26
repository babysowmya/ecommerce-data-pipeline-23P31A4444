CREATE TABLE warehouse.dim_customers(
    customer_key SERIAL PRIMARY KEY,    -- Surrogate key
    customer_id VARCHAR(20),            -- Business key
    full_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(20),
    customer_segment VARCHAR(20),       -- e.g., New / Regular / VIP
    registration_date DATE,
    effective_date DATE,                -- SCD Type 2
    end_date DATE,
    is_current BOOLEAN
);