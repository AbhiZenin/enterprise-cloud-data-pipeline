from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass(frozen=True)
class PipelineConfig:
    batch_id: str
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    quality_dir: Path
    currency: str
    max_null_percentage: float
    allow_negative_quantity: bool

def load_config(path: str = "config/pipeline.yml") -> PipelineConfig:
    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    pipeline = payload["pipeline"]
    quality = payload["quality"]
    return PipelineConfig(
        batch_id=str(pipeline["batch_id"]),
        raw_dir=Path(pipeline["raw_dir"]),
        bronze_dir=Path(pipeline["bronze_dir"]),
        silver_dir=Path(pipeline["silver_dir"]),
        gold_dir=Path(pipeline["gold_dir"]),
        quality_dir=Path(pipeline["quality_dir"]),
        currency=str(pipeline.get("currency", "USD")),
        max_null_percentage=float(quality.get("max_null_percentage", 5)),
        allow_negative_quantity=bool(quality.get("allow_negative_quantity", False)),
    )
