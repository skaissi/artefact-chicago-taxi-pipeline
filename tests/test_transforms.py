"""Spark transformation unit tests, executed in container (or local PySpark)."""
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from taxi_pipeline.transforms import aggregate_gold, transform_silver


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("taxi-unit-tests")
               .config("spark.sql.session.timeZone", "America/Chicago")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def sample(spark):
    columns = ["trip_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
               "trip_miles", "trip_total", "pickup_community_area"]
    rows = [
        ("id1", "2023-01-01T10:00:00.000", "2023-01-01T10:15:00.000", "600", "2", "12.00", "8"),
        ("id1", "2023-01-01T10:00:00.000", "2023-01-01T10:15:00.000", "600", "2", "12.00", "8"),
        ("id2", "2023-01-01T11:00:00.000", "2023-01-01T11:15:00.000", "300", "1", "8.00", None),
        ("id3", "2023-01-02T11:00:00.000", "2023-01-02T11:15:00.000", "120", "0.5", "5.00", "8"),
        ("id4", "2023-01-02T11:00:00.000", "2023-01-02T11:15:00.000", "-5", "1", "6.00", "8"),
        (None, "2023-01-02T11:00:00.000", "2023-01-02T11:15:00.000", "60", "1", "6.00", "8"),
        ("id5", "nonsense", "2023-01-02T11:15:00.000", "60", "1", "6.00", "8"),
    ]
    return spark.createDataFrame(rows, columns)


def test_clean_reject_dedup(sample):
    cleaned, rejected = transform_silver(sample)
    assert cleaned.count() == 3
    assert rejected.count() == 3
    assert {r.trip_id for r in cleaned.select("trip_id").collect()} == {"id1", "id2", "id3"}
    assert "invalid_duration" in {r.reject_reason for r in rejected.collect()}
    assert "invalid_start_timestamp" in {r.reject_reason for r in rejected.collect()}
    assert cleaned.filter("trip_id = 'id1'").first().duration_seconds == 600.0


def test_gold_reconciles(sample):
    cleaned, _ = transform_silver(sample)
    gold = aggregate_gold(cleaned)
    revenue = gold["daily_revenue"].collect()
    assert len(revenue) == 2
    assert sum(r.trip_count for r in revenue) == 3
    assert str(sum(r.total_revenue_usd for r in revenue)) == "25.00"
    assert {r.pickup_zone for r in gold["top_pickup_zones"].collect()} == {"8", "Unknown"}
    assert gold["daily_trip_metrics"].filter("trip_date = '2023-01-01'").first().avg_duration_min == 7.5


def test_required_fields_fail_fast(spark):
    df = spark.createDataFrame([("one",)], ["trip_id"])
    with pytest.raises(ValueError, match="Missing source columns"):
        transform_silver(df)
