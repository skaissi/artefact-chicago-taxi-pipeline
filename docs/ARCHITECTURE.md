# Architecture V2 — contrat de consommation / limites

```
Chicago SODA CSV -> MinIO bronze/<window>/batch=<UUID> (page bytes + SHA256)
  -> silver/<window>/batch=<UUID>/trips + rejected_records
  -> quality/chicago_taxi/rejected_records/      [historique d'exceptions, batch_id]
  -> silver/<window>/batch=<UUID>/_gold_candidates/{daily_revenue,daily_trip_metrics,top_pickup_zones}
  -> quality_gate [assert counts, revenue, zone rank, freshness, thresholds]
      | FAIL -> report validation JSON + quality history; STOP Gold (ancien conservé)
      | PASS -> publish_gold -> gold/chicago_taxi/{3 BUSINESS TABLES}
                           -> metadata/chicago_taxi/current_publication.json
Dremio S3 source (chicago_lake) -> 3 PDS Gold -> optional ChicagoTaxi business space views
                             -> 3 PDS quality -> optional ChicagoTaxi_Operations views
Power BI PBIP : business tabs + Data Quality + Rejection Explorer
```

- **Gold contient exactement trois tables métier dans le NOUVEAU préfixe**. Si le projet historique a déjà écrit `gold/chicago_taxi/start=...`, ces objets restent présents tant que l'utilisateur n'exécute pas volontairement le script d'archivage (guide migration).
- Bronze = fidélité à la réponse brute ; Silver = parsing, anomalies, déduplication, quarantaine **et candidats internes `_gold_candidates` / `_quality_tmp`** ; Gold = tables métier finales ; `quality/` = monitoring et rejets accessibles aux opérateurs, PAS un Data Product métier. Aucun préfixe `staging/` dédié n'est créé par cette version.
- `quality/chicago_taxi/quality_batch`, `quality_reasons`, `rejected_records` sont **cumulatifs et identifiés par batch_id** ; Dremio en fait trois datasets Parquet, et la page Power BI doit filtrer un batch ou assumer un agrégat historique.
- Les doublons sont présents dans `rejected_records` avec `reject_type=duplicate`, `reject_reason=duplicate_trip_id`. `rejected_rows` dans quality_batch compte uniquement les invalides ; `duplicate_valid_rows` compte les doublons. `quality_reasons` contient les deux catégories : sa somme = `rejected_rows + duplicate_valid_rows`.
- La fraîcheur compare `extracted_at_utc` de la requête API à l'heure du gate (24 h par défaut), **pas** la date des courses historiques de 2023.
- Les limites de valeurs réalistes (`MAX_TRIP_MILES`, `MAX_TRIP_DURATION_SECONDS`, `MAX_TRIP_TOTAL_USD`) sont des règles de démo à discuter avec le métier ; zone pickup : plages 1 à 77 (pas un contrôle sur des noms de zones). NULL pickup area autorisé et publié `Unknown` en Gold.
- Seule la fenêtre du **dernier run Airflow publié** est visible dans les trois tables : les dates du formulaire Airflow écrasent les défauts `.env` pour ce run. L'implémentation **n'est pas incrémentale** et ne promet pas un historique exhaustif de Chicago. `MAX_ROWS` limite la taille de l'échantillon ; aucune métrique n'est représentative de toutes les courses.
- Airflow `max_active_runs=1` limite les collisions intra-DAG, mais pas d'autres producteurs indépendants.
- **Publication non atomique** : malgré les candidats techniques dans Silver et le gate avant l'écrasement et le rollback *best effort*, les écritures Parquet écrasent plusieurs préfixes S3 séparément. Un lecteur concurrent peut voir une table ou un mélange transitoire, et un crash brutal peut laisser un Gold partiel. `current_publication.json` est écrit en dernier comme marqueur de réussite. Une transaction réellement atomique multi-table n'est PAS implémentée ; pour une architecture production utiliser des snapshots / catalog pointers transactionnels, ou Iceberg avec un catalogue adapté, plus isolation d'accès et tests d'intégration.
- `dremio/dremio-oss` est la couche SQL/catalogue fonctionnelle, sans gouvernance de type Unity Catalog, authentification isolée ni lineage certifié dans ce projet. Les identifiants MinIO sont ceux du compte root de **démo locale seulement** ; des politiques S3 séparées seraient nécessaires en production.
