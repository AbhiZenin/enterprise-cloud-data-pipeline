from datetime import datetime, timezone

import pandas as pd

from src.enterprise_pipeline.scd import apply_scd_type2


def test_scd_type2_closes_old_record_and_creates_new_record():
    existing = pd.DataFrame(
        [
            {
                "customer_id": "C001",
                "customer_name": "Asha Reddy",
                "email": "asha@example.com",
                "state": "TX",
                "signup_date": "2025-01-10",
                "effective_from": "2026-07-30T10:00:00+00:00",
                "effective_to": None,
                "is_current": True,
            }
        ]
    )

    incoming = pd.DataFrame(
        [
            {
                "customer_id": "C001",
                "customer_name": "Asha Reddy",
                "email": "asha@example.com",
                "state": "AZ",
                "signup_date": "2025-01-10",
            }
        ]
    )

    effective_time = datetime(
        2026,
        7,
        31,
        10,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result = apply_scd_type2(
        current_dimension=existing,
        incoming=incoming,
        business_key="customer_id",
        tracked_columns=[
            "customer_name",
            "email",
            "state",
        ],
        effective_time=effective_time,
    )

    customer_rows = result[
        result["customer_id"] == "C001"
    ]

    current_rows = customer_rows[
        customer_rows["is_current"] == True
    ]

    historical_rows = customer_rows[
        customer_rows["is_current"] == False
    ]

    assert len(customer_rows) == 2
    assert len(current_rows) == 1
    assert len(historical_rows) == 1

    assert current_rows.iloc[0]["state"] == "AZ"
    assert pd.isna(
        current_rows.iloc[0]["effective_to"]
    )

    assert historical_rows.iloc[0]["state"] == "TX"
    assert pd.Timestamp(
        historical_rows.iloc[0]["effective_to"]
    ) == pd.Timestamp(effective_time)