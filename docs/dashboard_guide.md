# Dashboard User Guide

## Access
- **Tableau Public:** [URL]  
- **Power BI Desktop:** dashboards/powerbi/ecommerce_analytics.pbix (requires Power BI Desktop)

## Dashboard Pages

### Page 1: Executive Summary
- **Purpose:** High-level business overview  
- **Metrics:** Total Revenue, Total Transactions, AOV, Profit Margin  
- **Visualizations:** Monthly Revenue Trend, Top Categories, Payment Distribution, State-wise map  
- **Filters:** Date range, Category, State, Payment Method

### Page 2: Product Analysis
- **Purpose:** Product performance insights  
- **Key Insights:** Top 10 products generate majority revenue, Electronics category has highest profit margin  
- **Visualizations:** Top products (bar), Category tree map, Price vs Profit scatter, Product metrics table

### Page 3: Customer Insights
- **Purpose:** Customer behavior analysis  
- **Key Insights:** VIP customers contribute 40% revenue, Retention rate insights  
- **Visualizations:** Customer segments (column), Acquisition trend (area), Top 10 customers (matrix), Purchase patterns (ribbon)

### Page 4: Geographic & Trends
- **Purpose:** Location and temporal patterns  
- **Key Insights:** Top 5 states → 70% revenue, Weekend sales higher than weekdays  
- **Visualizations:** State-wise revenue map, Top 10 states bar chart, Day-of-week line, Category revenue trend

## Filters Available
- Date Range  
- Product Category  
- Customer State  
- Payment Method

## Refreshing Data
1. Regenerate CSVs (run `generate_data.py`)  
2. Run full pipeline: `pipeline_orchestrator.py`  
3. Refresh Power BI / Tableau dashboard to load new data
```