# E-Commerce ETL & Analytics Pipeline

## Project Architecture
**Data Flow Diagram:**  
Raw CSV → Staging → Production → Warehouse → Analytics → BI Dashboard  

## Technology Stack
- **Data Generation:** Python (Faker)
- **Database:** PostgreSQL
- **ETL & Transformation:** Python (Pandas, SQLAlchemy)
- **Orchestration:** Python Scheduler
- **BI Tools:** Tableau Public / Power BI Desktop
- **Containerization:** Docker
- **Testing:** Pytest

## Project Structure
```

ETL_pipeline/
├── data/
│   ├── raw/
│   ├── staging/
│   └── processed/
├── dashboards/
│   ├── powerbi/
│   └── screenshots/
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── quality_checks/
│   ├── analytics/
│   ├── scheduler.py
│   └── pipeline_orchestrator.py
├── tests/
├── docs/
├── config.yaml
└── README.md

````

## Setup Instructions
1. Install Python dependencies:  
```bash
pip install -r requirements.txt
````

2. Start PostgreSQL database (Docker or local).
3. Create database `ecommerce_db` and schemas: `staging`, `production`, `warehouse`.
4. Update `config.yaml` with your DB credentials.

## Running the Pipeline

* **Full pipeline execution:**

```bash
python scripts/pipeline_orchestrator.py
```

* **Individual steps:**

```bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/analytics/generate_analytics.py
```

## Running Tests

```bash
bash run_tests.sh
# or
pytest tests/ -v
```

## Dashboard Access

* **Tableau Public URL:** [Your Tableau URL]
* **Power BI Desktop File:** dashboards/powerbi/ecommerce_analytics.pbix
* **Screenshots:** dashboards/screenshots/

## Database Schemas

**Staging Schema**

* `staging.customers`, `staging.products`, `staging.transactions`, `staging.transaction_items`

**Production Schema**

* `production.customers`, `production.products`, `production.transactions`, `production.transaction_items`

**Warehouse Schema**

* `warehouse.dim_customers`, `dim_products`, `dim_date`, `dim_payment_method`, `fact_sales`
* Aggregate tables: `agg_daily_sales`, `agg_product_performance`, `agg_customer_metrics`

## Key Insights from Analytics

* Top performing category: Electronics
* Monthly revenue trend: Peak in November
* VIP customers contribute 40% of revenue
* Top 5 states account for 70% of revenue
* Preferred payment method: Credit Card

## Challenges & Solutions

1. Handling SCD Type 2 → Surrogate keys & versioning
2. Ensuring idempotency → TRUNCATE & reload
3. Managing pipeline failures → Retry logic + circuit breaker

## Future Enhancements

* Real-time streaming with Apache Kafka
* Cloud deployment (AWS/GCP/Azure)
* ML models for revenue prediction
* Real-time alerting system