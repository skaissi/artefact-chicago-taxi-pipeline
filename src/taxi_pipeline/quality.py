"""Pure quality policy: thresholds apply to whole bounded API extracts, not to all Chicago trips."""
from datetime import datetime, timezone


def evaluate_quality(metrics, validation, manifest, *, max_reject_rate=0.10,
                     max_duplicate_rate=0.10, max_extract_age_hours=24,
                     now=None):
    problems = []
    ids = {metrics.get("batch_id"), validation.get("batch_id"), manifest.get("batch_id")}
    if None in ids or len(ids) != 1:
        problems.append("batch_id_mismatch")
    raw, clean = int(metrics["raw_rows"]), int(metrics["clean_rows"])
    invalid, duplicates = int(metrics["rejected_rows"]), int(metrics["duplicate_valid_rows"])
    delta = raw - clean - invalid - duplicates
    reject_rate = invalid / raw if raw else 1.0
    duplicate_rate = duplicates / raw if raw else 1.0
    if delta != 0:
        problems.append("raw_silver_reconciliation")
    if clean <= 0:
        problems.append("empty_clean_dataset")
    if reject_rate > max_reject_rate:
        problems.append("reject_rate_exceeded")
    if duplicate_rate > max_duplicate_rate:
        problems.append("duplicate_rate_exceeded")
    checked = now or datetime.now(timezone.utc)
    if checked.tzinfo is None:
        raise ValueError("now must be timezone aware")
    try:
        extracted = datetime.fromisoformat(manifest["extracted_at_utc"].replace("Z", "+00:00"))
        age_hours = (checked - extracted).total_seconds() / 3600
        if extracted.tzinfo is None or not 0 <= age_hours <= max_extract_age_hours:
            problems.append("stale_or_future_extraction")
    except (KeyError, TypeError, ValueError):
        age_hours = None
        problems.append("invalid_extraction_timestamp")
    if int(validation.get("silver_rows", -1)) != clean:
        problems.append("silver_materialization_mismatch")
    if int(validation.get("daily_count_sum", -1)) != clean:
        problems.append("daily_revenue_count_mismatch")
    if int(validation.get("metrics_count_sum", -1)) != clean:
        problems.append("daily_metrics_count_mismatch")
    if not validation.get("revenue_matches", False):
        problems.append("revenue_reconciliation")
    if not 1 <= int(validation.get("gold_top_zones_rows", 0)) <= 10:
        problems.append("top_zones_cardinality")
    if int(validation.get("null_zone_ranks", 1)) != 0:
        problems.append("null_zone_rank")
    return {"status": "FAIL" if problems else "PASS", "violations": problems,
            "reject_rate": reject_rate, "duplicate_rate": duplicate_rate,
            "reconciliation_delta": delta, "extract_age_hours": age_hours,
            "checked_at_utc": checked.isoformat()}
