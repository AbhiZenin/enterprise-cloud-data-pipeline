# Cloud Deployment Guide

## Azure landing zone

1. Create containers named `raw`, `bronze`, `silver`, and `gold` in Azure Data Lake Storage Gen2.
2. Configure a Databricks access connector or managed identity.
3. Replace local CSV reads with `spark.read.format("csv")`.
4. Persist each layer as Delta tables.
5. Schedule the notebook or Python wheel as a Databricks Workflow.

## Snowflake publishing

Create a curated database and schema:

```sql
CREATE DATABASE RETAIL_ANALYTICS;
CREATE SCHEMA RETAIL_ANALYTICS.GOLD;
```

Use the Snowflake connector from Databricks to load `DIM_CUSTOMER`, `DIM_PRODUCT`, and `FACT_SALES`.

## Power BI

Connect Power BI to Snowflake using DirectQuery or Import mode. Build:

- Executive KPI cards
- Revenue by category
- Daily revenue trend
- Customer and state drilldowns

## Production hardening

- Store secrets in Azure Key Vault
- Add Delta expectations or Great Expectations
- Configure Auto Loader for incremental ingestion
- Add audit tables and run metadata
- Enable Unity Catalog
- Add alerting through Azure Monitor
