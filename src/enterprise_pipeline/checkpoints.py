import hashlib
import json
from pathlib import Path


class CheckpointStore:
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, dataset: str) -> Path:
        return self.directory / f"{dataset}.json"

    def file_hash(self, file_path: Path) -> str:
        hasher = hashlib.sha256()

        with file_path.open("rb") as handle:
            while chunk := handle.read(8192):
                hasher.update(chunk)

        return hasher.hexdigest()

    def last_hash(self, dataset: str) -> str | None:
        path = self._path(dataset)

        if not path.exists():
            return None

        return json.loads(path.read_text()).get("last_hash")

    def is_changed(self, dataset: str, file_path: Path) -> bool:
        current_hash = self.file_hash(file_path)
        previous_hash = self.last_hash(dataset)

        return current_hash != previous_hash

    def mark_processed(self, dataset: str, file_path: Path):
        self._path(dataset).write_text(
            json.dumps(
                {
                    "last_hash": self.file_hash(file_path),
                },
                indent=2,
            )
        )