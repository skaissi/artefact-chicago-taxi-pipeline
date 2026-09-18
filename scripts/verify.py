"""Check the currently PUBLISHED run (not the .env default extraction window).

Airflow can override extraction dates per DagRun, so the old .env-window lookup
would accidentally verify an older/different manifest after a manual trigger.
"""
from taxi_pipeline.config import Settings
from taxi_pipeline.storage import load_manifest, s3_client


def main():
    cfg = Settings.from_env()
    client = s3_client(cfg)
    publication = load_manifest(client, cfg.bucket, "metadata/chicago_taxi/current_publication.json")
    batch = publication["batch_id"]
    manifest = load_manifest(client, cfg.bucket, f"metadata/chicago_taxi/batch={batch}/manifest.json")
    validation = load_manifest(client, cfg.bucket, f"metadata/chicago_taxi/batch={batch}/validation.json")
    if (validation["status"] != "PASS" or validation["batch_id"] != batch
            or manifest["batch_id"] != batch or publication.get("quality_gate") != "PASS"):
        raise SystemExit("FAIL: current published batch is not consistently validated")
    for name in ("daily_revenue", "daily_trip_metrics", "top_pickup_zones"):
        response = client.list_objects_v2(Bucket=cfg.bucket, Prefix=f"gold/chicago_taxi/{name}/")
        assert any(o["Key"].endswith(".parquet") for o in response.get("Contents", [])), name
    for name in ("rejected_records", "quality_batch", "quality_reasons"):
        response = client.list_objects_v2(Bucket=cfg.bucket, Prefix=f"quality/chicago_taxi/{name}/")
        assert any(o["Key"].endswith(".parquet") for o in response.get("Contents", [])), name
    print("PASS: published gate, marker, 3 business + 3 quality Parquet datasets")
    print(f"Published batch: {batch}; extraction window: "
          f"[{manifest['start_date_inclusive']}, {manifest['end_date_exclusive']})")


if __name__ == "__main__":
    main()
