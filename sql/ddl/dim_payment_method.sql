CREATE TABLE warehouse.dim_payment_method(
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(30),    -- e.g., "Credit Card"
    payment_type VARCHAR(20)            -- "Online" / "Offline"
);