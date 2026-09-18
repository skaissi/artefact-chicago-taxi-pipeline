"""Read Gold tables from MinIO and execute Spark SQL queries in sql/kpis.sql."""
from pathlib import Path
from taxi_pipeline.config import Settings
from taxi_pipeline.spark import s3_path, spark_session


def main():
    cfg = Settings.from_env()
    spark = spark_session(cfg)
    base = "gold/chicago_taxi"
    for name in ("daily_revenue", "daily_trip_metrics", "top_pickup_zones"):
        spark.read.parquet(s3_path(cfg, f"{base}/{name}")).createOrReplaceTempView(name)
    queries = (Path(__file__).resolve().parents[1] / "sql" / "kpis.sql").read_text(encoding="utf-8").split(";")
    for query in queries:
        query = query.strip()
        if query and not query.startswith("--"):
            print(f"\nSQL> {query}")
            spark.sql(query).show(30, truncate=False)


if __name__ == "__main__":
    main()
