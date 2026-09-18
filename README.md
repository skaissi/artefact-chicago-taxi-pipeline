# Artefact | Chicago Taxi Data Products — V2.1 local demo

**Une archive complète : Airflow 3 + PySpark 3.5.5 + PostgreSQL + MinIO + Dremio OSS + Power BI PBIP (4 pages).** Docker Compose exécute Airflow/Spark/MinIO/Dremio ; Power BI Desktop est un livrable Windows distinct dans `powerbi/`.

**Tu remplaces une installation existante ? LIS EN PREMIER `docs/MIGRATION_WINDOWS_FR.md`.** Conserve ton `.env`, le dossier Compose existant et les volumes Docker. Ne jamais faire `docker compose down -v`.

Démarrage après sauvegarde/remplacement des fichiers :

```powershell
Copy-Item .env.example .env        # UNIQUEMENT nouvelle installation ; ne pas écraser l'ancien .env
# Ajouter les variables MAX_* de .env.example à l'ancien .env en cas de migration.
docker compose up --build -d
docker compose ps -a
```

Interfaces locales : Airflow http://localhost:8080 ; MinIO http://localhost:9001 ; Dremio http://localhost:9047 (SQL `localhost:31010`). Credentials Dremio créés dans l'UI, credentials MinIO du fichier `.env`. Ne pas exposer ces ports ni ces credentials en production.

## Pipeline : 5 tâches (ancien DAG 4 tâches remplacé par 5)

`bronze_ingestion -> silver_processing -> gold_candidate -> quality_gate -> publish_gold`.

`quality_gate` PASS est une condition indispensable à `publish_gold`. Un FAIL conserve les trois Gold existants mais produit les métriques qualité et les rejets dans la zone opérationnelle. Les seuls **Data Products métier** dans le nouveau Gold :

```text
gold/chicago_taxi/daily_revenue/
gold/chicago_taxi/daily_trip_metrics/
gold/chicago_taxi/top_pickup_zones/
```

Le chemin des trois produits n'inclut plus `start=.../end=...`. La fenêtre et le batch sont enregistrés dans Bronze, Silver et metadata. Les **tables Gold candidates sont internes à Silver** sous `silver/<fenêtre>/batch=<id>/_gold_candidates/` : plus de zone `staging/` dédiée. Les métriques de travail sont sous `_quality_tmp/` dans le même batch Silver. La table opérationnelle demandée : `quality/chicago_taxi/rejected_records/` (invalides + doublons, batch_id + motifs). En complément `quality/chicago_taxi/quality_batch/` et `quality/chicago_taxi/quality_reasons/` contiennent l'historique DQ. Un ancien préfixe `gold/chicago_taxi/start=...` est **préservé tant que tu n'archives pas volontairement** (`docs/MIGRATION_WINDOWS_FR.md`).

## 1. Déclencher puis vérifier

Dans Airflow, `chicago_taxi_pipeline` → Trigger. Le formulaire propose `extract_start_date` (inclus, défaut `START_DATE` du `.env`) et `extract_end_date` (exclu, défaut `END_DATE`). Ces deux dates peuvent être modifiées **pour chaque exécution sans rebuild**. Ex. février 2023 : `2023-02-01` à `2023-03-01`. Le DAG conserve les cinq tâches ; après cinq tâches vertes :

```powershell
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify.py
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/query_gold.py
```

## 2. Dremio, requêtes et Power BI

**À faire une fois manuellement** : Dremio → source Amazon S3 `chicago_lake` connectée à `minio:9000`; promouvoir les SIX nouveaux dossiers Parquet; créer les deux Spaces `ChicagoTaxi` / `ChicagoTaxi_Operations`, puis exécuter `sql/dremio_views.sql`. Guide pas à pas : `docs/DREMIO_SETUP_FR.md`.

Les fichiers du **front Power BI 4 pages** sont sous `powerbi/ChicagoTaxi.pbip`. Ouvrir dans Desktop et remplacer les six tables M fictives par tes vues Dremio en conservant le schéma. Guide : `docs/POWERBI_CONNECTION_FR.md`. Aucune connexion ni fichier PBIX final ne sont promis sans test Windows.

## 3. Data quality / limitations

Voir `docs/DQ_RULES_FR.md` pour chaque règle codée et ses paramètres. `MAX_ROWS=150000` par défaut **n'est qu'un échantillon**, pas la totalité de Chicago. Les tables Gold sont des snapshots complets de la fenêtre courante, **non incrémentales**. Les trois réécritures Parquet après le gate restent **non atomiques au sens S3** : un consommateur en lecture pendant la publication peut voir un état mixte et un crash peut laisser des fichiers partiels. Voir `docs/ARCHITECTURE.md`.

Tests sans Spark : `PYTHONPATH=src pytest -q tests/test_quality_policy.py tests/test_ingestion.py tests/test_dag_contract.py tests/test_paths_contract.py`; tests PySpark : `docker compose exec airflow-scheduler pytest -q /opt/airflow/project/tests/test_transforms.py` (image reconstruite). Tests d'intégration Docker, Power BI et Dremio non exécutés dans cet environnement ; à valider chez toi. Guide précis d'activation : `docs/INSTALLATION_V2_1_WINDOWS_FR.md`.
