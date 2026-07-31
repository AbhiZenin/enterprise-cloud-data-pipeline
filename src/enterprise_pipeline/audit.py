from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
import json

@dataclass
class AuditRecord:
    pipeline_name: str
    batch_id: str
    dataset: str
    stage: str
    status: str
    input_rows: int
    output_rows: int
    rejected_rows: int
    started_at: str
    completed_at: str
    details: dict

def utc_now():
    return datetime.now(timezone.utc).isoformat()

class AuditWriter:
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def write(self, record: AuditRecord):
        path = self.directory / f"{record.batch_id}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(record), default=str) + "\n")
