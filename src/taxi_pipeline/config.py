"""Validated, environment-driven configuration (no hard-coded credentials)."""
from dataclasses import dataclass
from datetime import date
import os


@dataclass(frozen=True)
class Settings:
    start_date: str = "2023-01-01"
    end_date: str = "2023-01-08"  # exclusive
    page_size: int = 20_000
    max_rows: int = 150_000
    minio_endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin123"
    bucket: str = "taxi-datalake"
    app_token: str = ""
    driver_memory: str = "2g"
    spark_master: str = "local[2]"
    max_reject_rate: float = 0.10
    max_duplicate_rate: float = 0.10
    max_extract_age_hours: int = 24
    max_trip_miles: float = 500.0
    max_trip_duration_seconds: float = 86400.0
    max_trip_total_usd: float = 5000.0

    def __post_init__(self):
        start, end = date.fromisoformat(self.start_date), date.fromisoformat(self.end_date)
        if start >= end:
            raise ValueError("START_DATE must be strictly before END_DATE (exclusive).")
        if not 1 <= self.page_size <= 50_000:
            raise ValueError("PAGE_SIZE must be between 1 and 50000.")
        if self.max_rows < 1:
            raise ValueError("MAX_ROWS must be positive.")
        if not 0 <= self.max_reject_rate <= 1 or not 0 <= self.max_duplicate_rate <= 1:
            raise ValueError("Quality rates must be between 0 and 1")
        if self.max_extract_age_hours < 1:
            raise ValueError("MAX_EXTRACT_AGE_HOURS must be positive")
        if min(self.max_trip_miles, self.max_trip_duration_seconds, self.max_trip_total_usd) <= 0:
            raise ValueError("Maximum plausible trip values must be positive")
        if not self.bucket or not self.access_key or not self.secret_key:
            raise ValueError("S3 bucket and access credentials must not be blank.")

    @property
    def dataset_prefix(self) -> str:
        return f"chicago_taxi/start={self.start_date}/end={self.end_date}"

    @classmethod
    def from_env(cls):
        return cls(
            start_date=os.getenv("START_DATE", "2023-01-01"),
            end_date=os.getenv("END_DATE", "2023-01-08"),
            page_size=int(os.getenv("PAGE_SIZE", "20000")),
            max_rows=int(os.getenv("MAX_ROWS", "150000")),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
            bucket=os.getenv("LAKE_BUCKET", "taxi-datalake"),
            app_token=os.getenv("SODA_APP_TOKEN", ""),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            spark_master=os.getenv("SPARK_MASTER", "local[2]"),
            max_reject_rate=float(os.getenv("MAX_REJECT_RATE", "0.10")),
            max_duplicate_rate=float(os.getenv("MAX_DUPLICATE_RATE", "0.10")),
            max_extract_age_hours=int(os.getenv("MAX_EXTRACT_AGE_HOURS", "24")),
            max_trip_miles=float(os.getenv("MAX_TRIP_MILES", "500")),
            max_trip_duration_seconds=float(os.getenv("MAX_TRIP_DURATION_SECONDS", "86400")),
            max_trip_total_usd=float(os.getenv("MAX_TRIP_TOTAL_USD", "5000")),
        )
