# Architecture decision record

| Choice | Why | Trade-off |
| --- | --- | --- |
| Airflow 3 TaskFlow + LocalExecutor | Learn mainstream DAG orchestration, dependencies, retries, logs and XCom without Redis / Celery | All compute runs on one laptop; not production distributed processing |
| PostgreSQL metadata backend | Durable Airflow metadata, proper LocalExecutor backend | An additional container |
| Simple auth disabled | Low-friction, loopback-only teaching setup | Never expose this setup externally; use real auth in production |
| MinIO + Parquet | S3-compatible object storage; physical medallion layers | No atomic table commits |
| PySpark `local[2]` in Airflow task process | Actual Spark processing, simple one-command laptop demo | Driver shares memory with scheduler; production needs external Spark cluster/operator |
| SODA v2.1 CSV with order/window/offset | Bounded reproducible historic sample | Rate limits, offset cost, source updates between pages |
| Bronze unique-batch CSV + manifest | Keep original bytes and protect previously committed raw batch from failed ingestion | Orphan objects from failed attempts need cleanup |
| Silver rejects and deterministic dedup | Explicit auditable quality logic | Cannot claim completeness of source reporting |
| `trip_total` USD aggregation | Sum passenger-reported trip totals | Not platform profit or verified paid revenue |
| Fixed window overwrite Silver/Gold | Simpler repeatability, one DAG run at a time | No reader-safe atomic publication |
| Spark SQL over Gold Parquet | Actual SQL queries, no extra BI infrastructure | Not a permanently running SQL endpoint |

## With more time

- Run Spark on an isolated cluster via SparkSubmitOperator / a remote Spark service.
- Add versioned transactional tables (Iceberg/Delta), atomic promotion and per-run metadata.
- Add incremental watermark/keyset pagination, data-contract / schema drift tests and observability.
- Add area lookup dimension, Trino/warehouse endpoint and real dashboard.
- Use proper auth, generated secrets, image scanning, an external Airflow secrets manager and alerting.

## Business limitations

Trip timestamps are rounded in source: prefer `trip_seconds` for duration and use timestamp difference only if absent. Chicago reporting can be incomplete. Missing pickup zone is `Unknown` in the top-zone table. Rejected rows remain in auditable Silver Parquet. If the sample cap is reached, the oldest ordered subset of the selected period is processed; this is a sample, not citywide coverage.
