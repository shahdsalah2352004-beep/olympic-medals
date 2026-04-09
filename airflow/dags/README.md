 Olympic Medals Pipeline

 Overview
End-to-end data pipeline for Olympic medals data.

 Architecture
Scraping → ETL → dbt → Snowflake → Dashboard

 Steps
1. **Scraper** - scrapes Olympic medal data → CSV
2. **ETL** - loads CSV into Snowflake raw + star schema
3. **dbt** - transforms data into dimensions + fact table
4. **Dashboard** - Plotly Dash interactive dashboard

