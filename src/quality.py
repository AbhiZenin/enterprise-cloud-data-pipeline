from dataclasses import dataclass, asdict
from typing import Iterable
import pandas as pd

@dataclass
class QualityResult:
    dataset: str
    check_name: str
    passed: bool
    failed_rows: int
    details: str

def not_null_check(df: pd.DataFrame, dataset: str, columns: Iterable[str]) -> QualityResult:
    columns = list(columns)
    failed = int(df[columns].isna().any(axis=1).sum())
    return QualityResult(dataset, "not_null", failed == 0, failed, f"columns={columns}")

def unique_check(df: pd.DataFrame, dataset: str, columns: Iterable[str]) -> QualityResult:
    columns = list(columns)
    failed = int(df.duplicated(subset=columns, keep=False).sum())
    return QualityResult(dataset, "unique", failed == 0, failed, f"columns={columns}")

def positive_check(df: pd.DataFrame, dataset: str, column: str) -> QualityResult:
    failed = int((df[column] <= 0).sum())
    return QualityResult(dataset, "positive", failed == 0, failed, f"column={column}")

def referential_check(
    child: pd.DataFrame,
    parent: pd.DataFrame,
    dataset: str,
    child_column: str,
    parent_column: str,
) -> QualityResult:
    invalid = ~child[child_column].isin(parent[parent_column])
    failed = int(invalid.sum())
    return QualityResult(
        dataset,
        "referential_integrity",
        failed == 0,
        failed,
        f"{child_column}->{parent_column}",
    )

def results_frame(results):
    return pd.DataFrame([asdict(result) for result in results])
