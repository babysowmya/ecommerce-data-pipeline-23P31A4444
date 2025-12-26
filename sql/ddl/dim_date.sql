CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INT PRIMARY KEY,         -- Format YYYYMMDD
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);