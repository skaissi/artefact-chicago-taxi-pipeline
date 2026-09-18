"""Fast ingestion/config tests: no network, Docker, or Spark required."""
import csv
import io
import json
from unittest.mock import Mock

import pytest

from taxi_pipeline.config import Settings
from taxi_pipeline.ingest import SOURCE_URL, csv_rows, ingest_pages, query_params

HEADER = ["trip_id", "trip_start_timestamp", "trip_total", "trip_seconds"]


def raw_csv(ids):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADER)
    for trip_id in ids:
        writer.writerow([trip_id, "2023-01-01T00:00:00.000", "12.00", 300])
    return buf.getvalue().encode("utf-8")


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_bucket(self, **kwargs):
        return {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = Body


def test_settings_dates_and_limits():
    with pytest.raises(ValueError, match="strictly before"):
        Settings(start_date="2023-01-08", end_date="2023-01-01")
    with pytest.raises(ValueError, match="PAGE_SIZE"):
        Settings(page_size=50_001)
    with pytest.raises(ValueError, match="MAX_ROWS"):
        Settings(max_rows=0)


def test_query_is_windowed_ordered_paginated():
    p = query_params(Settings(), 100, 50)
    assert "trip_start_timestamp >= '2023-01-01T00:00:00'" in p["$where"]
    assert "trip_start_timestamp < '2023-01-08T00:00:00'" in p["$where"]
    assert p["$order"] == "trip_start_timestamp ASC, trip_id ASC"
    assert p["$offset"] == 100 and p["$limit"] == 50


def test_csv_multiline_and_bad_headers():
    assert csv_rows(raw_csv(["abc", "def"])) == 2
    with pytest.raises(ValueError, match="header"):
        csv_rows(b"{\"error\":\"rate limit\"}")
    with pytest.raises(ValueError, match="Malformed"):
        csv_rows(b"trip_id,trip_start_timestamp,trip_total\na,b\n")


def test_ingest_unchanged_pages_and_manifest(monkeypatch):
    monkeypatch.setattr("taxi_pipeline.ingest.ensure_bucket", lambda *_args: None)
    page1, page2 = raw_csv(["a", "b"]), raw_csv(["c"])
    client = FakeS3()
    session = Mock()
    session.get.side_effect = [Mock(content=page1), Mock(content=page2)]
    cfg = Settings(page_size=2, max_rows=10)
    result = ingest_pages(cfg, client=client, session=session)
    assert session.get.call_count == 2
    assert session.get.call_args_list[0].kwargs["params"]["$offset"] == 0
    assert session.get.call_args_list[1].kwargs["params"]["$offset"] == 2
    assert session.get.call_args_list[1].kwargs["params"]["$limit"] == 2
    assert result["rows_downloaded"] == 3 and result["pages"] == 2
    manifest = json.loads(client.objects[(cfg.bucket, result["manifest_key"])])
    assert client.objects[(cfg.bucket, manifest["pages"][0]["key"])] == page1
    assert client.objects[(cfg.bucket, manifest["pages"][1]["key"])] == page2
    assert manifest["pages"][0]["key"].startswith(f"bronze/{cfg.dataset_prefix}/batch=")
    assert manifest["rows_downloaded"] == 3
    assert manifest["capped"] is False
    assert len(manifest["pages"][0]["sha256"]) == 64


def test_cap_never_exceeded(monkeypatch):
    monkeypatch.setattr("taxi_pipeline.ingest.ensure_bucket", lambda *_args: None)
    cfg = Settings(page_size=2, max_rows=3)
    session = Mock()
    session.get.side_effect = [Mock(content=raw_csv(["a", "b"])), Mock(content=raw_csv(["c"]))]
    client = FakeS3()
    result = ingest_pages(cfg, client=client, session=session)
    assert result["rows_downloaded"] == 3
    assert session.get.call_args_list[-1].kwargs["params"]["$limit"] == 1
    assert json.loads(client.objects[(cfg.bucket, result["manifest_key"])])["capped"]


def test_no_manifest_if_api_fails(monkeypatch):
    monkeypatch.setattr("taxi_pipeline.ingest.ensure_bucket", lambda *_args: None)
    client = FakeS3()
    session = Mock()
    session.get.return_value.raise_for_status.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        ingest_pages(Settings(), client=client, session=session)
    assert client.objects == {}


def test_page_counts_quoted_newline():
    raw = b'trip_id,trip_start_timestamp,trip_total\n"id\npart",2023-01-01T00:00:00.000,10\n'
    assert csv_rows(raw) == 1


def test_failed_rerun_preserves_old_manifest(monkeypatch):
    monkeypatch.setattr("taxi_pipeline.ingest.ensure_bucket", lambda *_args: None)
    client = FakeS3()
    cfg = Settings(page_size=2, max_rows=3)
    initial = Mock()
    initial.get.side_effect = [Mock(content=raw_csv(["a", "b"])), Mock(content=raw_csv(["c"]))]
    first = ingest_pages(cfg, client=client, session=initial)
    manifest_bytes = client.objects[(cfg.bucket, first["manifest_key"])]
    first_page = json.loads(manifest_bytes)["pages"][0]["key"]
    original_data = client.objects[(cfg.bucket, first_page)]
    failed = Mock()
    failed.get.side_effect = [Mock(content=raw_csv(["other", "new"])), RuntimeError("network error")]
    with pytest.raises(RuntimeError, match="network error"):
        ingest_pages(cfg, client=client, session=failed)
    assert client.objects[(cfg.bucket, first["manifest_key"])] == manifest_bytes
    assert client.objects[(cfg.bucket, first_page)] == original_data
