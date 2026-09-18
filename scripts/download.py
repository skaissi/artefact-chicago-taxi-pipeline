"""Optional standalone SODA download + bronze upload (same code as Airflow)."""
from taxi_pipeline.config import Settings
from taxi_pipeline.ingest import ingest_pages


if __name__ == "__main__":
    print(ingest_pages(Settings.from_env()))
