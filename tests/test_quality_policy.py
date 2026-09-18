"""Pure Python DQ policy tests; no local Spark, Docker or S3 needed."""
from datetime import datetime, timedelta, timezone
from taxi_pipeline.quality import evaluate_quality

NOW = datetime(2026, 9, 17, 10, 0, tzinfo=timezone.utc)


def fixtures():
    q = dict(batch_id="b1", raw_rows=100, clean_rows=94,
             rejected_rows=4, duplicate_valid_rows=2)
    v = dict(batch_id="b1", silver_rows=94, daily_count_sum=94,
             metrics_count_sum=94, revenue_matches=True,
             gold_top_zones_rows=7, null_zone_ranks=0)
    m = dict(batch_id="b1", extracted_at_utc=(NOW-timedelta(minutes=3)).isoformat())
    return q, v, m


def check(q=None, v=None, m=None, **kwargs):
    x, y, z = fixtures()
    x.update(q or {}); y.update(v or {}); z.update(m or {})
    return evaluate_quality(x, y, z, now=NOW, **kwargs)


def test_pass():
    actual = check()
    assert actual['status'] == 'PASS'
    assert actual['reconciliation_delta'] == 0
    assert actual['violations'] == []


def test_reject_threshold():
    result = check(q=dict(rejected_rows=15, clean_rows=83),
                   v=dict(silver_rows=83, daily_count_sum=83, metrics_count_sum=83))
    assert 'reject_rate_exceeded' in result['violations']


def test_duplicate_threshold():
    result = check(q=dict(duplicate_valid_rows=12, clean_rows=84),
                   v=dict(silver_rows=84, daily_count_sum=84, metrics_count_sum=84))
    assert 'duplicate_rate_exceeded' in result['violations']


def test_reconciliation_and_revenue():
    result = check(q=dict(clean_rows=95), v=dict(revenue_matches=False))
    assert {'raw_silver_reconciliation', 'revenue_reconciliation'} <= set(result['violations'])


def test_stale_extraction_not_trip_date():
    _, _, m = fixtures()
    m['extracted_at_utc'] = (NOW - timedelta(days=3)).isoformat()
    assert 'stale_or_future_extraction' in check(m=m)['violations']


def test_future_extraction():
    assert 'stale_or_future_extraction' in check(m={
        'extracted_at_utc': (NOW+timedelta(hours=2)).isoformat()})['violations']


def test_cross_batch_guard():
    assert 'batch_id_mismatch' in check(v={'batch_id': 'b2'})['violations']


def test_empty_clean():
    result = check(q=dict(clean_rows=0, rejected_rows=98),
                   v=dict(silver_rows=0, daily_count_sum=0, metrics_count_sum=0))
    assert 'empty_clean_dataset' in result['violations']


def test_bad_timestamp():
    assert 'invalid_extraction_timestamp' in check(m={'extracted_at_utc': 'oops'})['violations']


def test_invalid_threshold_configuration():
    from taxi_pipeline.config import Settings
    import pytest
    with pytest.raises(ValueError, match='Quality rates'):
        Settings(max_reject_rate=1.2)
