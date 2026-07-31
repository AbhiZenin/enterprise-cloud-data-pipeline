from __future__ import annotations

from typing import Any

import pandas as pd


def apply_scd_type2(
    current_dimension: pd.DataFrame,
    incoming: pd.DataFrame,
    business_key: str,
    tracked_columns: list[str],
    effective_time: Any,
) -> pd.DataFrame:
    effective_timestamp = pd.Timestamp(effective_time)

    incoming = incoming.copy()
    current_dimension = current_dimension.copy()

    required_columns = [
        business_key,
        *tracked_columns,
    ]

    missing_incoming_columns = [
        column
        for column in required_columns
        if column not in incoming.columns
    ]

    if missing_incoming_columns:
        raise ValueError(
            "Incoming DataFrame is missing required columns: "
            f"{missing_incoming_columns}"
        )

    if current_dimension.empty:
        result = incoming.copy()

        result["effective_from"] = effective_timestamp
        result["effective_to"] = pd.NaT
        result["is_current"] = True

        return result.reset_index(drop=True)

    for column in [
        "effective_from",
        "effective_to",
        "is_current",
    ]:
        if column not in current_dimension.columns:
            if column == "effective_from":
                current_dimension[column] = effective_timestamp
            elif column == "effective_to":
                current_dimension[column] = pd.NaT
            else:
                current_dimension[column] = True

    current_dimension["effective_from"] = pd.to_datetime(
        current_dimension["effective_from"],
        errors="coerce",
    )

    current_dimension["effective_to"] = pd.to_datetime(
        current_dimension["effective_to"],
        errors="coerce",
    )

    current_dimension["is_current"] = (
        current_dimension["is_current"]
        .astype(str)
        .str.lower()
        .map(
            {
                "true": True,
                "false": False,
                "1": True,
                "0": False,
            }
        )
        .fillna(False)
        .astype(bool)
    )

    current_rows = current_dimension[
        current_dimension["is_current"]
    ].copy()

    historical_rows = current_dimension[
        ~current_dimension["is_current"]
    ].copy()

    current_lookup = current_rows.set_index(
        business_key,
        drop=False,
    )

    unchanged_rows: list[pd.Series] = []
    closed_rows: list[pd.Series] = []
    new_rows: list[pd.Series] = []

    for _, incoming_row in incoming.iterrows():
        key_value = incoming_row[business_key]

        if key_value not in current_lookup.index:
            new_row = incoming_row.copy()
            new_row["effective_from"] = effective_timestamp
            new_row["effective_to"] = pd.NaT
            new_row["is_current"] = True

            new_rows.append(new_row)
            continue

        existing_row = current_lookup.loc[key_value]

        if isinstance(existing_row, pd.DataFrame):
            existing_row = existing_row.iloc[0]

        has_changed = any(
            not _values_equal(
                existing_row[column],
                incoming_row[column],
            )
            for column in tracked_columns
        )

        if not has_changed:
            unchanged_rows.append(existing_row.copy())
            continue

        closed_row = existing_row.copy()
        closed_row["effective_to"] = effective_timestamp
        closed_row["is_current"] = False

        closed_rows.append(closed_row)

        new_row = incoming_row.copy()
        new_row["effective_from"] = effective_timestamp
        new_row["effective_to"] = pd.NaT
        new_row["is_current"] = True

        new_rows.append(new_row)

    incoming_keys = set(
        incoming[business_key].dropna().tolist()
    )

    untouched_current_rows = current_rows[
        ~current_rows[business_key].isin(incoming_keys)
    ].copy()

    pieces: list[pd.DataFrame] = []

    if not historical_rows.empty:
        pieces.append(historical_rows)

    if unchanged_rows:
        pieces.append(
            pd.DataFrame(unchanged_rows)
        )

    if closed_rows:
        pieces.append(
            pd.DataFrame(closed_rows)
        )

    if new_rows:
        pieces.append(
            pd.DataFrame(new_rows)
        )

    if not untouched_current_rows.empty:
        pieces.append(untouched_current_rows)

    non_empty_pieces = [
        piece
        for piece in pieces
        if not piece.empty
    ]

    if not non_empty_pieces:
        return pd.DataFrame(
            columns=[
                *incoming.columns,
                "effective_from",
                "effective_to",
                "is_current",
            ]
        )

    all_columns: list[str] = []

    for piece in non_empty_pieces:
        for column in piece.columns:
            if column not in all_columns:
                all_columns.append(column)

    prepared_pieces = [
        piece.dropna(
            axis=1,
            how="all",
        )
        for piece in non_empty_pieces
    ]

    result = pd.concat(
        prepared_pieces,
        ignore_index=True,
        sort=False,
    )

    result = result.reindex(
        columns=all_columns
    )

    result["effective_from"] = pd.to_datetime(
        result["effective_from"],
        errors="coerce",
    )

    result["effective_to"] = pd.to_datetime(
        result["effective_to"],
        errors="coerce",
    )

    result["is_current"] = (
        result["is_current"]
        .fillna(False)
        .astype(bool)
    )

    return result.reset_index(drop=True)


def _values_equal(
    left: Any,
    right: Any,
) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True

    if pd.isna(left) or pd.isna(right):
        return False

    if isinstance(left, pd.Timestamp):
        left = left.isoformat()

    if isinstance(right, pd.Timestamp):
        right = right.isoformat()

    return str(left) == str(right)