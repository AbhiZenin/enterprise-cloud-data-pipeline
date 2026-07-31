from __future__ import annotations
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from .config import PipelineConfig, load_config
from .quality import (
    not_null_check,
    unique_check,
    positive_check,
    referential_check,
    results_frame,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("enterprise_pipeline")

DATASETS = ("customers", "products", "sales")

def ensure_directories(config: PipelineConfig) -> None:
    for directory in (
        config.bronze_dir,
        config.silver_dir,
        config.gold_dir,
        config.quality_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

def ingest_bronze(config: PipelineConfig) -> dict[str, pd.DataFrame]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    frames: dict[str, pd.DataFrame] = {}
    for dataset in DATASETS:
        source = config.raw_dir / f"{dataset}.csv"
        frame = pd.read_csv(source)
        frame["_batch_id"] = config.batch_id
        frame["_ingested_at_utc"] = ingested_at
        frame["_source_file"] = source.name
        output = config.bronze_dir / f"{dataset}.csv"
        frame.to_csv(output, index=False)
        frames[dataset] = frame
        LOGGER.info("Bronze ingestion complete dataset=%s rows=%s", dataset, len(frame))
    return frames

def transform_silver(
    config: PipelineConfig,
    bronze: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    customers = bronze["customers"].copy()
    products = bronze["products"].copy()
    sales = bronze["sales"].copy()

    customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce")
    products["unit_price"] = pd.to_numeric(products["unit_price"], errors="coerce")
    sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
    sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")
    sales["unit_price"] = pd.to_numeric(sales["unit_price"], errors="coerce")

    customers = customers.drop_duplicates(subset=["customer_id"], keep="last")
    products = products.drop_duplicates(subset=["product_id"], keep="last")
    sales = sales.drop_duplicates(subset=["order_id"], keep="last")

    quality_results = [
        not_null_check(customers, "customers", ["customer_id", "customer_name", "email"]),
        unique_check(customers, "customers", ["customer_id"]),
        not_null_check(products, "products", ["product_id", "product_name", "unit_price"]),
        unique_check(products, "products", ["product_id"]),
        not_null_check(sales, "sales", ["order_id", "customer_id", "product_id", "quantity"]),
        unique_check(sales, "sales", ["order_id"]),
        positive_check(sales, "sales", "quantity"),
        referential_check(sales, customers, "sales", "customer_id", "customer_id"),
        referential_check(sales, products, "sales", "product_id", "product_id"),
    ]

    valid_customers = customers.dropna(subset=["customer_id", "customer_name", "email"])
    valid_products = products.dropna(subset=["product_id", "product_name", "unit_price"])
    valid_sales = sales[
        sales["customer_id"].isin(valid_customers["customer_id"])
        & sales["product_id"].isin(valid_products["product_id"])
        & sales["quantity"].notna()
        & sales["unit_price"].notna()
    ]
    if not config.allow_negative_quantity:
        valid_sales = valid_sales[valid_sales["quantity"] > 0]

    silver = {
        "customers": valid_customers,
        "products": valid_products,
        "sales": valid_sales,
    }
    for dataset, frame in silver.items():
        frame.to_csv(config.silver_dir / f"{dataset}.csv", index=False)
        LOGGER.info("Silver transformation complete dataset=%s rows=%s", dataset, len(frame))

    report = results_frame(quality_results)
    report.to_csv(config.quality_dir / "quality_report.csv", index=False)
    return silver, report

def build_gold(config: PipelineConfig, silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    customers = silver["customers"].copy()
    products = silver["products"].copy()
    sales = silver["sales"].copy()

    dim_customer = customers[
        ["customer_id", "customer_name", "email", "state", "signup_date"]
    ].copy()
    dim_product = products[
        ["product_id", "product_name", "category", "unit_price"]
    ].copy()

    fact_sales = (
        sales.merge(dim_customer[["customer_id", "customer_name", "state"]], on="customer_id")
        .merge(dim_product[["product_id", "product_name", "category"]], on="product_id")
    )
    fact_sales["revenue"] = fact_sales["quantity"] * fact_sales["unit_price"]

    kpis = pd.DataFrame(
        [
            {"metric": "total_revenue", "value": round(float(fact_sales["revenue"].sum()), 2)},
            {"metric": "total_orders", "value": int(fact_sales["order_id"].nunique())},
            {
                "metric": "average_order_value",
                "value": round(float(fact_sales.groupby("order_id")["revenue"].sum().mean()), 2),
            },
        ]
    )

    revenue_by_category = (
        fact_sales.groupby("category", as_index=False)["revenue"]
        .sum()
        .sort_values("revenue", ascending=False)
    )
    daily_revenue = (
        fact_sales.groupby("order_date", as_index=False)["revenue"]
        .sum()
        .sort_values("order_date")
    )

    gold = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "fact_sales": fact_sales,
        "kpis": kpis,
        "revenue_by_category": revenue_by_category,
        "daily_revenue": daily_revenue,
    }
    for dataset, frame in gold.items():
        frame.to_csv(config.gold_dir / f"{dataset}.csv", index=False)
        LOGGER.info("Gold model created dataset=%s rows=%s", dataset, len(frame))
    return gold

def run(config_path: str = "config/pipeline.yml") -> dict[str, pd.DataFrame]:
    config = load_config(config_path)
    ensure_directories(config)
    bronze = ingest_bronze(config)
    silver, quality_report = transform_silver(config, bronze)
    gold = build_gold(config, silver)
    failed_checks = int((~quality_report["passed"]).sum())
    LOGGER.info("Pipeline complete failed_quality_checks=%s", failed_checks)
    return gold

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yml")
    args = parser.parse_args()
    run(args.config)

if __name__ == "__main__":
    main()
