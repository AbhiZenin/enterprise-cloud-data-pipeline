from pathlib import Path
from pydantic import BaseModel, Field
import pandas as pd

class DatasetContract(BaseModel):
    name: str
    required_columns: list[str]
    primary_key: list[str]

CONTRACTS = {
    "customers": DatasetContract(
        name="customers",
        required_columns=["customer_id","customer_name","email","state","signup_date"],
        primary_key=["customer_id"],
    ),
    "products": DatasetContract(
        name="products",
        required_columns=["product_id","product_name","category","unit_price"],
        primary_key=["product_id"],
    ),
    "sales": DatasetContract(
        name="sales",
        required_columns=["order_id","order_date","customer_id","product_id","quantity","unit_price"],
        primary_key=["order_id"],
    ),
}

def validate_contract(dataset: str, frame: pd.DataFrame) -> list[str]:
    contract = CONTRACTS[dataset]
    errors = []
    missing = sorted(set(contract.required_columns) - set(frame.columns))
    if missing:
        errors.append(f"missing_columns={missing}")
    if not missing and frame[contract.primary_key].isna().any(axis=1).any():
        errors.append("null_primary_key")
    return errors
