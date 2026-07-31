from pathlib import Path
import pandas as pd
from src.quality import unique_check, positive_check

def test_unique_check_detects_duplicates():
    frame = pd.DataFrame({"id": [1, 1, 2]})
    result = unique_check(frame, "sample", ["id"])
    assert result.passed is False
    assert result.failed_rows == 2

def test_positive_check_detects_non_positive_values():
    frame = pd.DataFrame({"quantity": [2, 0, -1]})
    result = positive_check(frame, "sales", "quantity")
    assert result.failed_rows == 2
