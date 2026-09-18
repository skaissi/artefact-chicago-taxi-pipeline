# Data quality : règles V2 effectivement codées

## Bronze (ingest + build_silver)
1. Appels SODA ordonnés (`trip_start_timestamp`, `trip_id`), pagination bornée, réponses HTTP contrôlées, retries 429/5xx.
2. En-tête CSV minimum requis ; même nombre de colonnes par ligne ; aucune page > taille demandée ; aucune ingestion vide ; plafond MAX_ROWS.
3. CSV brute stockée **sans transformation**, nombre de lignes et SHA-256/bytes de chaque page en manifeste ; vérification de toutes les tailles et SHA-256 lors de la lecture Silver.
4. Validation colonne REQUIRED, chargement CSV FAILFAST et réconciliation du nombre de lignes avec manifeste.

## Silver — ligne
- Identifiant trip_id non vide ; timestamp de début convertible ; total en USD décimal non null, >=0 et <= `MAX_TRIP_TOTAL_USD` (5000 USD par défaut).
- Durée : trip_seconds sinon end-start ; non null, >=0 et <= `MAX_TRIP_DURATION_SECONDS` (86400 s). Si timestamps disponibles, `end >= start`, même si trip_seconds > 0.
- Distance NULL autorisée, sinon >=0 et <= `MAX_TRIP_MILES` (500 mi).
- `pickup_community_area` NULL autorisé (`Unknown` en Gold), sinon entier entre 1 et 77. Les chaînes non numériques deviennent NULL à la conversion et sont actuellement autorisées comme Unknown : ceci n'est PAS un vrai contrôle référentiel exhaustif.
- Motifs de rejet multi-valeurs séparés par `;`; valeurs invalides stockées dans `silver/.../rejected_records` et une copie opérationnelle dans `quality/chicago_taxi/rejected_records`.
- Déduplication par trip_id, tri « complétude > date de fin récente > hash stable ». Un doublon éliminé est aussi visible dans la table de rejets avec `reject_type=duplicate` et `reject_reason=duplicate_trip_id`.
- Réconciliation : raw = clean + invalid + duplicates ; clean > 0.

## Gold candidat et gate AVANT publication
- 3 tables candidates temporaires dans `silver/.../batch=.../_gold_candidates/`, jamais exposées aux consommateurs avant validation.
- Relectures Silver/candidats, somme des trip_count des DEUX agrégats journaliers = clean_count, somme revenue Silver = somme revenue Gold, Top 10 entre 1 et 10 et rank non NULL.
- Seuils configurables : invalid/raw <= `MAX_REJECT_RATE` (10 %), duplicates/raw <= `MAX_DUPLICATE_RATE` (10 %), fraîcheur d'extraction <= `MAX_EXTRACT_AGE_HOURS` (24 h) et non dans le futur, batch_id identique.
- Gate FAIL : écriture `metadata/chicago_taxi/batch=<UUID>/validation.json` status FAIL + raisons et une ligne `quality_batch` + `quality_reasons`, puis échec Airflow ; **Gold existant non touché**. Les échecs plus précoces (ex. CSV structure ou IO exception pendant Spark) peuvent ne pas produire ce rapport complet.
- Gate PASS : crée les rapports et les tables qualité, puis une tâche séparée `publish_gold` écrit les 3 tables publiques dans les chemins stables ; marqueur de publication à la fin. Voir limite non-atomicité dans ARCHITECTURE.md.

## Tables qualité dans Dremio (dossiers format Parquet)
- `quality/chicago_taxi/rejected_records` : détail des lignes invalides ET doublons, batch_id, reject_type, reject_reason. Historique des tentatives qui ont franchi Silver, y compris gate FAIL.
- `quality/chicago_taxi/quality_batch` : une ligne par batch par passage de gate, PASS ou FAIL, rates + violations, recorded/checked timestamps.
- `quality/chicago_taxi/quality_reasons` : une ligne par combinaison de motifs observée par batch ; plusieurs motifs par ligne sont une combinaison, pas un total par code atomique.

## Non implémenté / limites de métier
- Pas de vrai catalogue géographique des zones / correspondance ID → nom, pas de test source complet exhaustif, pas d'alerte mail/Teams, pas de test de distribution ou drift, pas de masquage PII, pas de publication multi-table atomique, pas de format Delta/Iceberg, pas d'incrémental. Thresholds arbitraires et non approuvés métier.
