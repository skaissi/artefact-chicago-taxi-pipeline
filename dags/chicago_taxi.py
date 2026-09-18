"""Airflow 3 orchestration: per-run extraction window and gated Gold publication."""
from dataclasses import replace
import os

import pendulum
from airflow.sdk import Param, dag, get_current_context, task


def _settings_for_run():
    """Use the *executing DagRun* values, not the parse-time DAG defaults.

    All five tasks resolve exactly the same window via the task context.
    Settings.__post_init__ validates ISO dates and start < exclusive end.
    """
    from taxi_pipeline.config import Settings

    params = get_current_context()["params"]
    return replace(
        Settings.from_env(),
        start_date=params["extract_start_date"],
        end_date=params["extract_end_date"],
    )


@dag(
    dag_id="chicago_taxi_pipeline",
    description="Bronze -> Silver + technical candidates -> quality gate -> publish",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, catchup=False, max_active_runs=1,
    tags=["artefact", "pyspark", "minio", "data-products", "quality"],
    params={
        "extract_start_date": Param(
            os.getenv("START_DATE", "2023-01-01"),
            type="string", format="date", title="Début d'extraction (inclus)",
            description="Premier jour des courses à extraire, inclus (AAAA-MM-JJ).",
        ),
        "extract_end_date": Param(
            os.getenv("END_DATE", "2023-01-08"),
            type="string", format="date", title="Fin d'extraction (exclue)",
            description="Premier jour hors extraction, exclu (AAAA-MM-JJ).",
        ),
    },
)
def chicago_taxi_pipeline():
    @task(retries=2, retry_delay=pendulum.duration(minutes=1))
    def bronze_ingestion() -> dict:
        from taxi_pipeline.ingest import ingest_pages
        return ingest_pages(_settings_for_run())

    @task(retries=1)
    def silver_processing(bronze: dict) -> dict:
        from taxi_pipeline.pipeline import build_silver
        return build_silver(_settings_for_run(), bronze["manifest_key"])

    @task(retries=1)
    def gold_candidate(silver: dict) -> dict:
        from taxi_pipeline.pipeline import build_gold_candidate
        return build_gold_candidate(_settings_for_run(), silver)

    @task(retries=0)
    def quality_gate(bronze: dict, silver: dict, candidate: dict) -> dict:
        from taxi_pipeline.pipeline import validate_pipeline
        return validate_pipeline(_settings_for_run(), silver, candidate, bronze["manifest_key"])

    @task(retries=0)
    def publish_gold(candidate: dict, gate: dict) -> dict:
        from taxi_pipeline.pipeline import publish_gold as publish
        return publish(_settings_for_run(), candidate, gate)

    bronze = bronze_ingestion()
    silver = silver_processing(bronze)
    candidate = gold_candidate(silver)
    gate = quality_gate(bronze, silver, candidate)
    publish_gold(candidate, gate)


chicago_taxi_pipeline()
