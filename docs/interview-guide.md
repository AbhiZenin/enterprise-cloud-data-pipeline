# Interview Guide

## Problem

Operational files were inconsistent and duplicated, preventing reliable reporting.

## Solution

I implemented a Medallion Architecture pipeline. Bronze preserved raw records with ingestion metadata, Silver standardized schemas and enforced validation, and Gold produced dimensional models and KPIs.

## Important design decisions

- Immutable Bronze layer for traceability
- Idempotent deduplication by business keys
- Referential-integrity checks before Gold
- Quarantine/reporting instead of silently dropping bad data
- Separate dimensions and facts for BI performance

## Metrics to discuss

- Number of rows processed
- Percentage of rejected records
- Runtime by stage
- Data-quality check pass rate
- Dashboard query latency
