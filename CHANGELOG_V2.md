# V2 — modifications réelles par rapport au ZIP initial

- Correction image MinIO : `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`.
- Dremio OSS intégré au Compose (volume `dremio_data`, interface 9047, port client SQL 31010).
- Bronze : manifeste conservé sous une clé immuable par `batch_id`, avec lien latest historique ; vérification réelle SHA-256 + octets à la relecture.
- Silver : dossiers par run, règles de validation étendues et valeurs max configurables ; doublons conservés en exceptions auditées, motif `duplicate_trip_id`.
- Zone opérationnelle `quality/chicago_taxi` hors Gold : `rejected_records`, `quality_batch` et `quality_reasons` Parquet stables et historisés par `batch_id`. Copie idempotente par objet MinIO ; les échecs avant Silver peuvent ne pas produire de métriques complètes.
- Tâches DAG = 5, ordre : ingestion → silver → candidate Gold staging → quality gate avec seuils/réconciliation/fraîcheur → publication finale.
- Gold métier nouvelle convention stable : seulement `daily_revenue`, `daily_trip_metrics`, `top_pickup_zones` dans `gold/chicago_taxi`. Ancien Gold daté non effacé automatiquement.
- Historique d'audit et publication `metadata/chicago_taxi/current_publication.json` ; retour arrière de fichiers best effort, **pas de transaction atomique multi-table**.
- Dremio : guide source MinIO, promotions physiques + Spaces/vues SQL pour navigation métier et opérationnelle, refresh métadonnées.
- Power BI : PBIP 4 pages / 49 visuels / 7 tables dont `Fact_Rejected_Records`; six sources M de démo à connecter à Dremio ; aucune ouverture réelle Power BI Desktop garantie.
- Scripts : `verify.py`, `query_gold.py` chemins stables, `archive_legacy_gold.py` en mode dry-run par défaut, `validate_pbip.py` test statique.
- Tests locaux sans PySpark : tests DQ + ingestion + structure DAG. Tests intégration Spark/Docker/Dremio et ouverture Windows encore à faire sur le PC utilisateur.
