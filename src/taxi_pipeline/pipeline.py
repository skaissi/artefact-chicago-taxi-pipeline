"""Batch-isolated Silver -> technical Gold candidates -> quality gate -> publication.

This is a deliberately bounded local demo. Plain Parquet overwrites on S3 are NOT atomic;
see docs/ARCHITECTURE.md before offering concurrent readers / SLAs.
"""
from datetime import datetime, timezone
import hashlib

from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType)

from taxi_pipeline.quality import evaluate_quality
from taxi_pipeline.spark import s3_path, spark_session
from taxi_pipeline.storage import load_manifest, s3_client, save_manifest
from taxi_pipeline.transforms import REQUIRED, aggregate_gold, transform_silver_detailed

GOLD_NAMES = ("daily_revenue", "daily_trip_metrics", "top_pickup_zones")


def candidate_prefix(settings, batch_id):
    """Unpublished Gold candidates live in the technical part of Silver, not Gold."""
    return f"silver/{settings.dataset_prefix}/batch={batch_id}/_gold_candidates"


def copy_parquet_to_flat_prefix(client, bucket, source_prefix, destination_prefix, batch_id):
    """Deterministic destinations make a retried copy idempotent; exclude _SUCCESS/CRC."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=source_prefix.rstrip("/") + "/"):
        keys += [item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".parquet")]
    if not keys:
        raise RuntimeError(f"No Parquet files under {source_prefix}")
    for index, key in enumerate(sorted(keys)):
        client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key},
                           Key=f"{destination_prefix.rstrip('/')}/batch-{batch_id}-{index:04d}.parquet")
    return len(keys)


def delete_prefix(client, bucket, prefix):
    """Delete only a curated prefix, never the bucket or any Docker volume."""
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def build_silver(settings, manifest_key):
    client = s3_client(settings)
    manifest = load_manifest(client, settings.bucket, manifest_key)
    if manifest["rows_downloaded"] != sum(p["rows"] for p in manifest["pages"]):
        raise AssertionError("Bronze manifest has inconsistent row counts")
    # Check immutable CSV byte integrity against the ingestion manifest before Spark reads.
    for page in manifest["pages"]:
        body = client.get_object(Bucket=settings.bucket, Key=page["key"])["Body"].read()
        if len(body) != page["bytes"] or hashlib.sha256(body).hexdigest() != page["sha256"]:
            raise AssertionError(f"Bronze page integrity check failed: {page['key']}")
    spark = spark_session(settings)
    paths = [s3_path(settings, p["key"]) for p in manifest["pages"]]
    raw = spark.read.option("header", "true").option("multiLine", "true").option("mode", "FAILFAST").csv(paths)
    if missing := sorted(REQUIRED.difference(raw.columns)):
        raise ValueError(f"Missing columns in Bronze: {missing}")
    raw_count = raw.count()
    if raw_count != manifest["rows_downloaded"]:
        raise AssertionError(f"CSV rows {raw_count} != manifest {manifest['rows_downloaded']}")

    clean, invalid, duplicate = transform_silver_detailed(
        raw, max_trip_miles=settings.max_trip_miles,
        max_trip_duration_seconds=settings.max_trip_duration_seconds,
        max_trip_total_usd=settings.max_trip_total_usd)
    clean, invalid, duplicate = clean.cache(), invalid.cache(), duplicate.cache()
    try:
        clean_count, invalid_count, duplicate_count = clean.count(), invalid.count(), duplicate.count()
        if not clean_count or raw_count != clean_count + invalid_count + duplicate_count:
            raise AssertionError("Silver reconciliation failed or Silver empty")
        batch = manifest["batch_id"]
        prefix = f"silver/{settings.dataset_prefix}/batch={batch}"
        clean.write.mode("overwrite").parquet(s3_path(settings, f"{prefix}/trips"))
        all_rejections = (invalid.withColumn("reject_type", F.lit("invalid"))
                          .unionByName(duplicate.withColumn("reject_type", F.lit("duplicate")))
                          .withColumn("batch_id", F.lit(batch)).cache())
        try:
            breakdown = {r["reject_reason"]: r["count"] for r in
                         all_rejections.groupBy("reject_reason").count().collect()}
            rejection_key = f"{prefix}/rejected_records"
            all_rejections.coalesce(1).write.mode("overwrite").parquet(s3_path(settings, rejection_key))
            # Operational history is independent of the business Gold quality gate.
            copy_parquet_to_flat_prefix(client, settings.bucket, rejection_key,
                                        "quality/chicago_taxi/rejected_records", batch)
        finally:
            all_rejections.unpersist()
        quality = {
            "batch_id": batch, "raw_rows": raw_count, "clean_rows": clean_count,
            "rejected_rows": invalid_count, "duplicate_valid_rows": duplicate_count,
            "reject_breakdown": breakdown,
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        quality_key = f"metadata/chicago_taxi/batch={batch}/quality.json"
        save_manifest(client, settings.bucket, quality_key, quality)
        # Retain previous legacy report path for existing docs, but immutable batch key is authoritative.
        save_manifest(client, settings.bucket, f"metadata/{settings.dataset_prefix}/quality.json", quality)
        return {"silver_key": f"{prefix}/trips", "rejected_key": rejection_key,
                "quality_key": quality_key, **quality}
    finally:
        clean.unpersist()
        invalid.unpersist()
        duplicate.unpersist()


def build_gold_candidate(settings, silver):
    """Build three Silver-scoped candidates without modifying consumer-visible Gold."""
    spark = spark_session(settings)
    clean = spark.read.parquet(s3_path(settings, silver["silver_key"])).cache()
    try:
        count = clean.count()
        if count != silver["clean_rows"]:
            raise AssertionError("Materialized Silver row count differs from quality metrics")
        prefix = candidate_prefix(settings, silver["batch_id"])
        for name, df in aggregate_gold(clean).items():
            df.coalesce(1).write.mode("overwrite").parquet(s3_path(settings, f"{prefix}/{name}"))
        return {"batch_id": silver["batch_id"], "candidate_prefix": prefix, "silver_rows": count}
    finally:
        clean.unpersist()


def _audit_schema():
    strings = {"batch_id", "recorded_at_utc", "checked_at_utc", "gate_status", "violations"}
    doubles = {"reject_rate", "duplicate_rate", "extract_age_hours"}
    names = ["batch_id", "recorded_at_utc", "checked_at_utc", "gate_status",
             "raw_rows", "clean_rows", "rejected_rows", "duplicate_valid_rows",
             "reconciliation_delta", "gold_daily_rows", "gold_top_zones_rows",
             "reject_rate", "duplicate_rate", "extract_age_hours", "violations"]
    return StructType([StructField(n, StringType() if n in strings else
                                   DoubleType() if n in doubles else LongType(), True) for n in names])


def _write_quality_history(settings, client, spark, silver, validation):
    """Store one version per batch; copying only Parquet objects avoids _SUCCESS in Dremio."""
    batch = silver["batch_id"]
    metric = dict(batch_id=batch, recorded_at_utc=silver["recorded_at_utc"],
                  checked_at_utc=validation["checked_at_utc"], gate_status=validation["status"],
                  raw_rows=int(silver["raw_rows"]), clean_rows=int(silver["clean_rows"]),
                  rejected_rows=int(silver["rejected_rows"]),
                  duplicate_valid_rows=int(silver["duplicate_valid_rows"]),
                  reconciliation_delta=int(validation["reconciliation_delta"]),
                  gold_daily_rows=int(validation.get("gold_daily_rows", 0)),
                  gold_top_zones_rows=int(validation.get("gold_top_zones_rows", 0)),
                  reject_rate=float(validation["reject_rate"]),
                  duplicate_rate=float(validation["duplicate_rate"]),
                  extract_age_hours=validation["extract_age_hours"],
                  violations=";".join(validation["violations"]))
    schema = _audit_schema()
    row = tuple(metric[c.name] for c in schema.fields)
    technical = f"silver/{settings.dataset_prefix}/batch={batch}/_quality_tmp"
    spark.createDataFrame([row], schema).coalesce(1).write.mode("overwrite").parquet(
        s3_path(settings, f"{technical}/quality_batch"))
    copy_parquet_to_flat_prefix(client, settings.bucket, f"{technical}/quality_batch",
                                "quality/chicago_taxi/quality_batch", batch)
    reason_schema = "batch_id string, reject_reason string, reject_count long"
    reason_rows = [(batch, str(reason), int(count)) for reason, count in sorted(silver["reject_breakdown"].items())]
    spark.createDataFrame(reason_rows, reason_schema).coalesce(1).write.mode("overwrite").parquet(
        s3_path(settings, f"{technical}/quality_reasons"))
    copy_parquet_to_flat_prefix(client, settings.bucket, f"{technical}/quality_reasons",
                                "quality/chicago_taxi/quality_reasons", batch)


def validate_pipeline(settings, silver, candidate, manifest_key):
    """Assert the gate BEFORE publishing any curated Gold; retain FAIL audit and raise."""
    client = s3_client(settings)
    manifest = load_manifest(client, settings.bucket, manifest_key)
    if candidate["batch_id"] != silver["batch_id"]:
        raise AssertionError("Candidate / Silver batch mismatch")
    spark = spark_session(settings)
    prefix = candidate["candidate_prefix"]
    s = spark.read.parquet(s3_path(settings, silver["silver_key"]))
    rev = spark.read.parquet(s3_path(settings, f"{prefix}/daily_revenue"))
    metrics = spark.read.parquet(s3_path(settings, f"{prefix}/daily_trip_metrics"))
    zones = spark.read.parquet(s3_path(settings, f"{prefix}/top_pickup_zones"))
    silver_count = s.count()
    sum_rev = rev.agg(F.sum("trip_count").alias("n")).first()["n"]
    sum_metrics = metrics.agg(F.sum("trip_count").alias("n")).first()["n"]
    raw_revenue = s.agg(F.sum("trip_total_usd").alias("v")).first()["v"]
    gold_revenue = rev.agg(F.sum("total_revenue_usd").alias("v")).first()["v"]
    validation = {
        "batch_id": silver["batch_id"], "silver_rows": silver_count,
        "daily_count_sum": sum_rev, "metrics_count_sum": sum_metrics,
        "revenue_matches": raw_revenue == gold_revenue,
        "gold_daily_rows": rev.count(), "gold_top_zones_rows": zones.count(),
        "null_zone_ranks": zones.filter(F.col("rank").isNull()).count(),
        "revenue_usd": str(gold_revenue),
    }
    result = evaluate_quality(silver, validation, manifest,
                              max_reject_rate=settings.max_reject_rate,
                              max_duplicate_rate=settings.max_duplicate_rate,
                              max_extract_age_hours=settings.max_extract_age_hours)
    report = {**validation, **result}
    key = f"metadata/chicago_taxi/batch={silver['batch_id']}/validation.json"
    save_manifest(client, settings.bucket, key, report)
    save_manifest(client, settings.bucket, f"metadata/{settings.dataset_prefix}/validation.json", report)
    _write_quality_history(settings, client, spark, silver, report)
    if report["status"] != "PASS":
        raise AssertionError("Quality gate FAIL: " + ", ".join(report["violations"]))
    return {"batch_id": silver["batch_id"], "status": "PASS", "validation_key": key,
            "candidate_prefix": prefix}


def publish_gold(settings, candidate, gate):
    """Local demo publication: write stable business prefixes only after gate PASS.

    Best-effort rollback from previous immutable candidate on failure. S3/Parquet
    multi-table publication is NOT atomic, even with this precaution.
    """
    if (gate.get("status") != "PASS" or gate.get("batch_id") != candidate.get("batch_id")
            or gate.get("candidate_prefix") != candidate.get("candidate_prefix")
            or candidate.get("candidate_prefix") != candidate_prefix(settings, candidate.get("batch_id"))):
        raise PermissionError("Gold publication requires matching PASS report and Silver-scoped candidate")
    client = s3_client(settings)
    report = load_manifest(client, settings.bucket, gate["validation_key"])
    if report["status"] != "PASS" or report["batch_id"] != candidate["batch_id"]:
        raise PermissionError("Validation report does not match candidate")
    publication_key = "metadata/chicago_taxi/current_publication.json"
    try:
        previous = load_manifest(client, settings.bucket, publication_key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            previous = None
        else:
            raise
    if previous and previous.get("batch_id") == candidate["batch_id"]:
        return previous
    spark = spark_session(settings)

    def copy_tables(prefix):
        for name in GOLD_NAMES:
            source = s3_path(settings, f"{prefix}/{name}")
            dest = s3_path(settings, f"gold/chicago_taxi/{name}")
            spark.read.parquet(source).coalesce(1).write.mode("overwrite").parquet(dest)

    try:
        copy_tables(candidate["candidate_prefix"])
        published = {"batch_id": candidate["batch_id"],
                     "candidate_prefix": candidate["candidate_prefix"],
                     "published_at_utc": datetime.now(timezone.utc).isoformat(),
                     "quality_gate": "PASS", "tables": list(GOLD_NAMES)}
        save_manifest(client, settings.bucket, publication_key, published)
        return published
    except Exception:
        if previous and previous.get("candidate_prefix"):
            try:
                copy_tables(previous["candidate_prefix"])
            except Exception as rollback_error:
                raise RuntimeError("Publication failed AND rollback failed; consumers must stop reading Gold") from rollback_error
        else:
            for name in GOLD_NAMES:
                delete_prefix(client, settings.bucket, f"gold/chicago_taxi/{name}")
        raise
