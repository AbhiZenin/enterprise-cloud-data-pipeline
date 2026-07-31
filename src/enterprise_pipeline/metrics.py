from prometheus_client import Counter, Histogram

ROWS_PROCESSED = Counter(
    "pipeline_rows_processed_total",
    "Rows processed by pipeline stage",
    ["dataset", "stage"],
)
ROWS_REJECTED = Counter(
    "pipeline_rows_rejected_total",
    "Rows rejected by pipeline stage",
    ["dataset", "stage"],
)
STAGE_DURATION = Histogram(
    "pipeline_stage_duration_seconds",
    "Pipeline stage duration",
    ["dataset", "stage"],
)
