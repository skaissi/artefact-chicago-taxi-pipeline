"""Fetch stable, paginated SODA CSV pages and preserve byte-for-byte CSVs."""
import csv
import hashlib
import io
import uuid
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from taxi_pipeline.storage import ensure_bucket, s3_client, save_manifest

SOURCE_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"


def build_session():
    session = requests.Session()
    # GET is idempotent; explicit handling for rate limits and transient server errors.
    retry = Retry(
        total=5, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]), respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def query_params(settings, offset, limit):
    return {
        "$where": (
            f"trip_start_timestamp >= '{settings.start_date}T00:00:00' "
            f"AND trip_start_timestamp < '{settings.end_date}T00:00:00'"
        ),
        # Both keys create a reproducible ordering; trip_id is unique in the source.
        "$order": "trip_start_timestamp ASC, trip_id ASC",
        "$limit": limit,
        "$offset": offset,
    }


def csv_rows(raw):
    """Validate CSV response and count logical rows (not newline bytes)."""
    text = raw.decode("utf-8-sig")
    stream = csv.reader(io.StringIO(text, newline=""))
    header = next(stream, None)
    if not header or not {"trip_id", "trip_start_timestamp", "trip_total"}.issubset(header):
        raise ValueError("Unexpected Socrata CSV header (or HTTP error payload).")
    count = 0
    for row in stream:
        if len(row) != len(header):
            raise ValueError("Malformed CSV page: column count does not match header.")
        count += 1
    return count


def ingest_pages(settings, client=None, session=None):
    client = client or s3_client(settings)
    session = session or build_session()
    ensure_bucket(client, settings.bucket)
    # Unique batch paths make each Bronze snapshot immutable: a failed rerun
    # cannot overwrite objects referenced by the previous committed manifest.
    batch_id = uuid.uuid4().hex
    prefix = f"bronze/{settings.dataset_prefix}/batch={batch_id}"
    manifest_key = f"metadata/{settings.dataset_prefix}/manifest.json"
    pages, total, offset = [], 0, 0
    headers = {"X-App-Token": settings.app_token} if settings.app_token else {}

    while total < settings.max_rows:
        limit = min(settings.page_size, settings.max_rows - total)
        response = session.get(
            SOURCE_URL, params=query_params(settings, offset, limit),
            headers=headers, timeout=(15, 180),
        )
        response.raise_for_status()
        raw = response.content
        count = csv_rows(raw)
        if not count:
            break
        if count > limit:
            raise ValueError("API returned more records than requested.")
        key = f"{prefix}/page-{len(pages):05d}.csv"
        client.put_object(
            Bucket=settings.bucket, Key=key, Body=raw,
            ContentType="text/csv; charset=utf-8",
        )
        pages.append({
            "key": key, "rows": count, "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        })
        total += count
        offset += count
        if count < limit:
            break

    if not pages:
        raise ValueError("No taxi trips returned for the selected period; no data published.")

    # A manifest is committed only after ALL pages have been successfully uploaded.
    # Readers use only manifest-listed files, never wildcard a possibly stale prefix.
    manifest = {
        "source_url": SOURCE_URL,
        "batch_id": batch_id,
        "start_date_inclusive": settings.start_date,
        "end_date_exclusive": settings.end_date,
        "order_by": "trip_start_timestamp ASC, trip_id ASC",
        "page_size": settings.page_size,
        "max_rows": settings.max_rows,
        "rows_downloaded": total,
        "capped": total == settings.max_rows,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "pages": pages,
    }
    immutable_manifest_key = f"metadata/chicago_taxi/batch={batch_id}/manifest.json"
    save_manifest(client, settings.bucket, immutable_manifest_key, manifest)
    save_manifest(client, settings.bucket, manifest_key, manifest)
    return {"manifest_key": immutable_manifest_key, "batch_id": batch_id,
            "rows_downloaded": total, "pages": len(pages)}
