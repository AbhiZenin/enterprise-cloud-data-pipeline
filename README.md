# Enterprise Lakehouse Data Platform

A production-style retail lakehouse demonstrating batch and incremental ingestion, Medallion Architecture, data contracts, SCD Type 2 dimensions, audit metadata, data-quality gates, observability, and cloud deployment patterns.

## Architecture

```text
Azure Blob / Local Landing
          |
          v
Ingestion Service -- schema contract -- dead-letter zone
          |
          v
Bronze Delta-compatible layer
          |
          v
Silver standardization + dedupe + CDC merge + quality gates
          |
          v
Gold dimensional model + SCD Type 2 + KPI marts
          |
          +--> Snowflake publishing adapter
          +--> Power BI semantic layer
          +--> Audit and lineage metadata
```

## Enterprise capabilities

- Incremental file discovery using checkpoints
- Idempotent batch processing
- Data-contract validation
- Dead-letter and quarantine handling
- SCD Type 2 customer dimension
- Partition-aware output layout
- Audit tables with row counts and status
- Structured JSON logging
- Retry policies
- Prometheus-compatible metrics
- Local Spark-compatible design
- Databricks, Azure Blob, Snowflake adapters
- Terraform, Kubernetes, CI/CD, and runbooks

## Local quick start

```bash
docker compose up --build
```

Or:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.enterprise_pipeline.cli run --config config/enterprise.yml
pytest -q
```

## Production scope

The repository contains deployment-ready templates. Cloud resources are not provisioned until valid credentials and billing access are supplied.
