# Architecture

The platform separates ingestion, standardization, dimensional modeling, publishing, and audit concerns. Each batch receives a unique ID. Raw records remain immutable in Bronze. Invalid records are quarantined. Silver applies type normalization and referential validation. Gold uses SCD Type 2 for customer history and produces analytics facts and KPIs.
