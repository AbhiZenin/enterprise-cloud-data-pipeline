from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path

import pandas as pd
import yaml

from .audit import AuditRecord, AuditWriter, utc_now
from .checkpoints import CheckpointStore
from .contracts import CONTRACTS, validate_contract
from .publishers import LocalCsvPublisher
from .scd import apply_scd_type2


LOGGER = logging.getLogger(__name__)


class EnterprisePipeline:
    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

        pipeline_config = self.config["pipeline"]

        self.pipeline_name = pipeline_config["name"]
        self.batch_id = (
            f"{pipeline_config['batch_id_prefix']}-"
            f"{uuid.uuid4().hex[:10]}"
        )

        self.landing = Path(pipeline_config["landing_dir"])
        self.bronze = Path(pipeline_config["bronze_dir"])
        self.silver = Path(pipeline_config["silver_dir"])
        self.gold = Path(pipeline_config["gold_dir"])
        self.quarantine = Path(pipeline_config["quarantine_dir"])

        self.checkpoints = CheckpointStore(
            pipeline_config["checkpoint_dir"]
        )

        self.audit = AuditWriter(
            pipeline_config["audit_dir"]
        )

        for directory in [
            self.landing,
            self.bronze,
            self.silver,
            self.gold,
            self.quarantine,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    def _read_csv(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(
                f"Input file does not exist: {path}"
            )

        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                return pd.read_csv(path)
            except (OSError, IOError) as error:
                last_error = error

                LOGGER.warning(
                    "csv_read_failed path=%s attempt=%s error=%s",
                    path,
                    attempt,
                    error,
                )

                if attempt < 3:
                    time.sleep(2 ** (attempt - 1))

        if last_error is not None:
            raise last_error

        raise RuntimeError(
            f"Unable to read CSV file: {path}"
        )

    def _latest_bronze_file(
        self,
        dataset: str,
    ) -> Path:
        bronze_files = list(
            self.bronze.glob(f"{dataset}_*.csv")
        )

        if not bronze_files:
            raise RuntimeError(
                "No Bronze file found for unchanged dataset "
                f"'{dataset}'. Delete its checkpoint file and rerun."
            )

        return max(
            bronze_files,
            key=lambda file_path: file_path.stat().st_mtime,
        )

    def ingest_dataset(
        self,
        dataset: str,
    ) -> pd.DataFrame:
        path = self.landing / f"{dataset}.csv"
        started_at = utc_now()

        if not path.exists():
            raise FileNotFoundError(
                f"Landing file does not exist: {path}"
            )

        if not self.checkpoints.is_changed(
            dataset,
            path,
        ):
            latest_bronze_path = (
                self._latest_bronze_file(dataset)
            )

            LOGGER.info(
                "dataset_skipped dataset=%s reason=no_changes "
                "bronze_path=%s",
                dataset,
                latest_bronze_path,
            )

            frame = self._read_csv(
                latest_bronze_path
            )

            self.audit.write(
                AuditRecord(
                    pipeline_name=self.pipeline_name,
                    batch_id=self.batch_id,
                    dataset=dataset,
                    stage="bronze",
                    status="SKIPPED",
                    input_rows=len(frame),
                    output_rows=len(frame),
                    rejected_rows=0,
                    started_at=started_at,
                    completed_at=utc_now(),
                    details={
                        "reason": "no_changes_detected",
                        "reused_bronze_path": str(
                            latest_bronze_path
                        ),
                    },
                )
            )

            return frame

        frame = self._read_csv(path)
        errors = validate_contract(
            dataset,
            frame,
        )

        if errors:
            quarantine_path = (
                self.quarantine
                / f"{dataset}_{self.batch_id}.csv"
            )

            frame.to_csv(
                quarantine_path,
                index=False,
            )

            self.audit.write(
                AuditRecord(
                    pipeline_name=self.pipeline_name,
                    batch_id=self.batch_id,
                    dataset=dataset,
                    stage="bronze",
                    status="FAILED",
                    input_rows=len(frame),
                    output_rows=0,
                    rejected_rows=len(frame),
                    started_at=started_at,
                    completed_at=utc_now(),
                    details={
                        "errors": errors,
                        "quarantine_path": str(
                            quarantine_path
                        ),
                    },
                )
            )

            raise ValueError(
                "Contract validation failed for "
                f"{dataset}: {errors}"
            )

        frame["_batch_id"] = self.batch_id
        frame["_ingested_at_utc"] = utc_now()

        bronze_path = (
            self.bronze
            / f"{dataset}_{self.batch_id}.csv"
        )

        frame.to_csv(
            bronze_path,
            index=False,
        )

        self.audit.write(
            AuditRecord(
                pipeline_name=self.pipeline_name,
                batch_id=self.batch_id,
                dataset=dataset,
                stage="bronze",
                status="SUCCESS",
                input_rows=len(frame),
                output_rows=len(frame),
                rejected_rows=0,
                started_at=started_at,
                completed_at=utc_now(),
                details={
                    "source_path": str(path),
                    "output_path": str(bronze_path),
                },
            )
        )

        # Save the checkpoint only after successful validation,
        # Bronze publishing and audit logging.
        self.checkpoints.mark_processed(
            dataset,
            path,
        )

        LOGGER.info(
            "dataset_ingested dataset=%s rows=%s "
            "bronze_path=%s",
            dataset,
            len(frame),
            bronze_path,
        )

        return frame

    def transform(
        self,
        frames: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame | int]:
        transformation_started_at = utc_now()

        customers = frames["customers"].copy()
        products = frames["products"].copy()
        sales = frames["sales"].copy()

        duplicate_customers = customers[
            customers.duplicated(
                subset=["customer_id"],
                keep="first",
            )
        ].copy()

        duplicate_products = products[
            products.duplicated(
                subset=["product_id"],
                keep="first",
            )
        ].copy()

        duplicate_sales = sales[
            sales.duplicated(
                subset=["order_id"],
                keep="first",
            )
        ].copy()

        customers = customers.drop_duplicates(
            subset=["customer_id"],
            keep="first",
        ).copy()

        products = products.drop_duplicates(
            subset=["product_id"],
            keep="first",
        ).copy()

        sales = sales.drop_duplicates(
            subset=["order_id"],
            keep="first",
        ).copy()

        customers["signup_date"] = pd.to_datetime(
            customers["signup_date"],
            errors="coerce",
        )

        products["unit_price"] = pd.to_numeric(
            products["unit_price"],
            errors="coerce",
        )

        sales["order_date"] = pd.to_datetime(
            sales["order_date"],
            errors="coerce",
        )

        sales["quantity"] = pd.to_numeric(
            sales["quantity"],
            errors="coerce",
        )

        sales["unit_price"] = pd.to_numeric(
            sales["unit_price"],
            errors="coerce",
        )

        invalid_customers = customers[
            customers["email"].isna()
            | customers["customer_id"].isna()
            | customers["signup_date"].isna()
        ].copy()

        invalid_products = products[
            products["product_id"].isna()
            | products["unit_price"].isna()
            | (products["unit_price"] <= 0)
        ].copy()

        valid_customers = customers[
            ~customers.index.isin(
                invalid_customers.index
            )
        ].copy()

        valid_products = products[
            ~products.index.isin(
                invalid_products.index
            )
        ].copy()

        invalid_sales = sales[
            sales["order_id"].isna()
            | sales["order_date"].isna()
            | sales["quantity"].isna()
            | sales["unit_price"].isna()
            | (sales["quantity"] <= 0)
            | (sales["unit_price"] <= 0)
            | (
                ~sales["customer_id"].isin(
                    valid_customers["customer_id"]
                )
            )
            | (
                ~sales["product_id"].isin(
                    valid_products["product_id"]
                )
            )
        ].copy()

        valid_sales = sales[
            ~sales.index.isin(
                invalid_sales.index
            )
        ].copy()

        quarantine_frames = {
            "customers": invalid_customers,
            "products": invalid_products,
            "sales": invalid_sales,
            "duplicate_customers": duplicate_customers,
            "duplicate_products": duplicate_products,
            "duplicate_sales": duplicate_sales,
        }

        for (
            quarantine_name,
            quarantine_frame,
        ) in quarantine_frames.items():
            if quarantine_frame.empty:
                continue

            quarantine_path = (
                self.quarantine
                / f"{quarantine_name}_{self.batch_id}.csv"
            )

            quarantine_frame.to_csv(
                quarantine_path,
                index=False,
            )

        valid_frames = {
            "customers": valid_customers,
            "products": valid_products,
            "sales": valid_sales,
        }

        input_frames = {
            "customers": customers,
            "products": products,
            "sales": sales,
        }

        duplicate_counts = {
            "customers": len(duplicate_customers),
            "products": len(duplicate_products),
            "sales": len(duplicate_sales),
        }

        invalid_counts = {
            "customers": len(invalid_customers),
            "products": len(invalid_products),
            "sales": len(invalid_sales),
        }

        for dataset_name, frame in valid_frames.items():
            silver_path = (
                self.silver
                / f"{dataset_name}_{self.batch_id}.csv"
            )

            frame.to_csv(
                silver_path,
                index=False,
            )

            total_rejected = (
                invalid_counts[dataset_name]
                + duplicate_counts[dataset_name]
            )

            self.audit.write(
                AuditRecord(
                    pipeline_name=self.pipeline_name,
                    batch_id=self.batch_id,
                    dataset=dataset_name,
                    stage="silver",
                    status="SUCCESS",
                    input_rows=(
                        len(input_frames[dataset_name])
                        + duplicate_counts[dataset_name]
                    ),
                    output_rows=len(frame),
                    rejected_rows=total_rejected,
                    started_at=transformation_started_at,
                    completed_at=utc_now(),
                    details={
                        "invalid_rows": (
                            invalid_counts[dataset_name]
                        ),
                        "duplicate_rows": (
                            duplicate_counts[dataset_name]
                        ),
                        "output_path": str(
                            silver_path
                        ),
                        "quality_validation": (
                            "completed"
                        ),
                    },
                )
            )

        total_invalid_rows = sum(
            invalid_counts.values()
        )

        total_duplicate_rows = sum(
            duplicate_counts.values()
        )

        return {
            "customers": valid_customers,
            "products": valid_products,
            "sales": valid_sales,
            "rejected_rows": (
                total_invalid_rows
                + total_duplicate_rows
            ),
        }

    def build_gold(
        self,
        silver: dict[str, pd.DataFrame | int],
    ) -> dict[str, pd.DataFrame]:
        effective_time = utc_now()

        customers = silver["customers"]
        products = silver["products"]
        sales = silver["sales"]
        rejected_rows = silver["rejected_rows"]

        if not isinstance(customers, pd.DataFrame):
            raise TypeError(
                "Silver customers must be a DataFrame"
            )

        if not isinstance(products, pd.DataFrame):
            raise TypeError(
                "Silver products must be a DataFrame"
            )

        if not isinstance(sales, pd.DataFrame):
            raise TypeError(
                "Silver sales must be a DataFrame"
            )

        if not isinstance(rejected_rows, int):
            raise TypeError(
                "Rejected rows must be an integer"
            )

        dim_customer_path = (
            self.gold / "dim_customer.csv"
        )

        if dim_customer_path.exists():
            current_dimension = pd.read_csv(
                dim_customer_path
            )
        else:
            current_dimension = pd.DataFrame()

        if (
            not current_dimension.empty
            and "is_current"
            not in current_dimension.columns
        ):
            current_dimension["effective_from"] = (
                effective_time
            )
            current_dimension["effective_to"] = None
            current_dimension["is_current"] = True

        dim_customer = apply_scd_type2(
            current_dimension=current_dimension,
            incoming=customers[
                [
                    "customer_id",
                    "customer_name",
                    "email",
                    "state",
                    "signup_date",
                ]
            ],
            business_key="customer_id",
            tracked_columns=[
                "customer_name",
                "email",
                "state",
            ],
            effective_time=effective_time,
        )

        dim_product = products.copy()

        current_customers = dim_customer[
            dim_customer["is_current"] == True
        ][
            [
                "customer_id",
                "customer_name",
                "state",
            ]
        ]

        fact_sales = (
            sales.merge(
                current_customers,
                on="customer_id",
                how="inner",
            )
            .merge(
                dim_product[
                    [
                        "product_id",
                        "product_name",
                        "category",
                    ]
                ],
                on="product_id",
                how="inner",
            )
        )

        fact_sales["revenue"] = (
            fact_sales["quantity"]
            * fact_sales["unit_price"]
        )

        daily_revenue = (
            fact_sales.groupby(
                "order_date",
                as_index=False,
            )["revenue"]
            .sum()
            .sort_values("order_date")
        )

        revenue_by_category = (
            fact_sales.groupby(
                "category",
                as_index=False,
            )["revenue"]
            .sum()
            .sort_values(
                "revenue",
                ascending=False,
            )
        )

        kpis = pd.DataFrame(
            [
                {
                    "metric": "total_revenue",
                    "value": round(
                        float(
                            fact_sales[
                                "revenue"
                            ].sum()
                        ),
                        2,
                    ),
                },
                {
                    "metric": "total_orders",
                    "value": int(
                        fact_sales[
                            "order_id"
                        ].nunique()
                    ),
                },
                {
                    "metric": "rejected_rows",
                    "value": rejected_rows,
                },
            ]
        )

        publisher = LocalCsvPublisher(
            str(self.gold)
        )

        gold_frames = {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "fact_sales": fact_sales,
            "daily_revenue": daily_revenue,
            "revenue_by_category": (
                revenue_by_category
            ),
            "kpis": kpis,
        }

        for table_name, frame in gold_frames.items():
            publisher.publish(
                table_name,
                frame,
            )

        LOGGER.info(
            "gold_layer_published tables=%s",
            list(gold_frames),
        )

        return gold_frames

    def run(self) -> dict[str, pd.DataFrame]:
        LOGGER.info(
            "pipeline_started batch_id=%s",
            self.batch_id,
        )

        try:
            frames = {
                dataset_name: self.ingest_dataset(
                    dataset_name
                )
                for dataset_name in CONTRACTS
            }

            silver = self.transform(frames)
            gold = self.build_gold(silver)

            LOGGER.info(
                "pipeline_completed batch_id=%s",
                self.batch_id,
            )

            return gold

        except Exception:
            LOGGER.exception(
                "pipeline_failed batch_id=%s",
                self.batch_id,
            )
            raise