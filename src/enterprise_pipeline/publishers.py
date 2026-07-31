from pathlib import Path
import pandas as pd

class Publisher:
    def publish(self, table_name: str, frame: pd.DataFrame) -> None:
        raise NotImplementedError

class LocalCsvPublisher(Publisher):
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def publish(self, table_name: str, frame: pd.DataFrame) -> None:
        frame.to_csv(self.directory / f"{table_name}.csv", index=False)

class SnowflakePublisher(Publisher):
    def __init__(self, *args, **kwargs):
        self.config = kwargs

    def publish(self, table_name: str, frame: pd.DataFrame) -> None:
        raise RuntimeError(
            "Snowflake publishing requires credentials and the snowflake connector. "
            "Configure secrets and replace this adapter in deployment."
        )
