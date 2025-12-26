# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture of the e-commerce analytics platform.

## System Components
1. **Data Generation Layer**
   - Generates synthetic CSVs (customers, products, transactions, transaction_items) using Python Faker

2. **Data Ingestion Layer**
   - Loads CSVs into `staging` schema using Python + psycopg2
   - Batch ingestion approach

3. **Data Storage Layer**
   - **Staging:** Raw CSVs, minimal validation  
   - **Production:** Cleaned, normalized data (3NF)  
   - **Warehouse:** Star schema with dimensions, fact, aggregates

4. **Data Processing Layer**
   - Data quality checks  
   - Transformations (cleansing, enrichment)  
   - Dimensional modeling (SCD Type 2)  
   - Aggregate tables for performance

5. **Data Serving Layer**
   - Analytical SQL queries  
   - Pre-computed aggregations  
   - BI tool connectivity

6. **Visualization Layer**
   - Tableau Public / Power BI  
   - 16+ interactive visualizations across 4 pages

7. **Orchestration Layer**
   - Pipeline orchestrator  
   - Scheduler for daily runs  
   - Monitoring & alerting

## Data Models
### Staging
- Exact CSV replica  
- Minimal validation

### Production
- 3NF normalized tables  
- Foreign key constraints  
- Data integrity enforced

### Warehouse
- Star schema: 4 dimension tables, 1 fact table, 3 aggregates  
- Optimized for analytical queries  
- SCD Type 2 implemented for historical tracking

## Deployment Architecture
- Dockerized ETL scripts  
- PostgreSQL container  
- Scheduler container triggers pipeline daily  
- Monitoring logs and JSON reports stored locally
````