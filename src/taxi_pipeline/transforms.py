"""Deterministic, side-effect-free transformations. All trip amounts are reported USD."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REQUIRED = {
    "trip_id", "trip_start_timestamp", "trip_end_timestamp",
    "trip_seconds", "trip_miles", "trip_total", "pickup_community_area",
}


def transform_silver_detailed(raw: DataFrame, *, max_trip_miles=500.0,
                              max_trip_duration_seconds=86400.0, max_trip_total_usd=5000.0):
    """Return (clean, invalid, duplicate). Duplicates have reason duplicate_trip_id.

    Null pickup areas and distances are permitted; negative distances are not.
    trip_seconds is authoritative where available (source timestamps are rounded).
    """
    missing = sorted(REQUIRED.difference(raw.columns))
    if missing:
        raise ValueError(f"Missing source columns: {missing}")
    parsed = (
        raw.select(
            F.trim("trip_id").alias("trip_id"),
            F.to_timestamp("trip_start_timestamp").alias("trip_start_ts"),
            F.to_timestamp("trip_end_timestamp").alias("trip_end_ts"),
            F.col("trip_seconds").cast("double").alias("trip_seconds"),
            F.col("trip_miles").cast("double").alias("trip_miles"),
            F.col("trip_total").cast("decimal(18,2)").alias("trip_total_usd"),
            F.col("pickup_community_area").cast("int").alias("pickup_community_area"),
        )
        .withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn(
            "duration_seconds",
            F.coalesce(F.col("trip_seconds"),
                       F.unix_timestamp("trip_end_ts") - F.unix_timestamp("trip_start_ts")),
        )
    )
    reasons = [
        F.when(F.col("trip_id").isNull() | (F.col("trip_id") == ""), "missing_trip_id"),
        F.when(F.col("trip_start_ts").isNull(), "invalid_start_timestamp"),
        F.when(F.col("trip_total_usd").isNull() | (F.col("trip_total_usd") < 0), "invalid_trip_total"),
        F.when(F.col("duration_seconds").isNull() | (F.col("duration_seconds") < 0), "invalid_duration"),
        F.when(F.col("trip_miles").isNotNull() & (F.col("trip_miles") < 0), "negative_trip_miles"),
        F.when(F.col("trip_end_ts").isNotNull() & F.col("trip_start_ts").isNotNull()
               & (F.col("trip_end_ts") < F.col("trip_start_ts")), "end_before_start"),
        F.when(F.col("trip_miles") > max_trip_miles, "implausible_trip_miles"),
        F.when(F.col("duration_seconds") > max_trip_duration_seconds, "implausible_duration"),
        F.when(F.col("trip_total_usd") > max_trip_total_usd, "implausible_trip_total"),
        F.when(F.col("pickup_community_area").isNotNull()
               & ~F.col("pickup_community_area").between(1, 77), "invalid_pickup_area"),
    ]
    classified = parsed.withColumn("reject_reason", F.concat_ws(";", *reasons))
    invalid = classified.filter(F.col("reject_reason") != "")
    valid = classified.filter(F.col("reject_reason") == "").drop("reject_reason")
    fields = ["trip_end_ts", "trip_miles", "pickup_community_area"]
    w = Window.partitionBy("trip_id").orderBy(
        F.col("completeness").desc(),
        F.col("trip_end_ts").desc_nulls_last(),
        F.col("row_hash").asc(),
    )
    ranked = (
        valid.withColumn("completeness", sum(
            [F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in fields]
        ))
        .withColumn("row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in valid.columns])), 256))
        .withColumn("rn", F.row_number().over(w))
    )
    cleaned = ranked.filter(F.col("rn") == 1).drop("completeness", "row_hash", "rn")
    duplicates = (
        ranked.filter(F.col("rn") > 1)
        .drop("completeness", "row_hash", "rn")
        .withColumn("reject_reason", F.lit("duplicate_trip_id"))
    )
    return cleaned, invalid, duplicates


def transform_silver(raw: DataFrame):
    """Backward-compatible two-result API used by the original unit tests."""
    clean, invalid, _duplicates = transform_silver_detailed(raw)
    return clean, invalid


def aggregate_gold(cleaned: DataFrame):
    daily_revenue = (
        cleaned.groupBy("trip_date")
        .agg(F.count("trip_id").alias("trip_count"),
             F.sum("trip_total_usd").cast("decimal(20,2)").alias("total_revenue_usd"))
        .orderBy("trip_date")
    )
    daily_trip_metrics = (
        cleaned.groupBy("trip_date")
        .agg(F.count("trip_id").alias("trip_count"),
             F.round(F.avg("duration_seconds") / 60, 2).alias("avg_duration_min"),
             F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"))
        .orderBy("trip_date")
    )
    zone_counts = (
        cleaned.withColumn("pickup_zone", F.coalesce(
            F.col("pickup_community_area").cast("string"), F.lit("Unknown")))
        .groupBy("pickup_zone")
        .agg(F.count("trip_id").alias("trip_count"),
             F.sum("trip_total_usd").cast("decimal(20,2)").alias("total_revenue_usd"))
    )
    w = Window.orderBy(F.desc("trip_count"), F.asc("pickup_zone"))
    top_zones = (
        zone_counts.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
        .select("rank", "pickup_zone", "trip_count", "total_revenue_usd")
        .orderBy("rank")
    )
    return {"daily_revenue": daily_revenue,
            "daily_trip_metrics": daily_trip_metrics,
            "top_pickup_zones": top_zones}
