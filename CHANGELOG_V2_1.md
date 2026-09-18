# V2.1 — Date de déclenchement Airflow et candidats techniques Silver

- Ajout de deux `Param` Airflow dans `dags/chicago_taxi.py` : `extract_start_date` inclus et `extract_end_date` exclu ; valeurs par défaut lues dans `.env` pendant l'analyse du DAG. Les cinq tâches lisent le contexte du run, et Settings valide dates / ordre.
- Candidats Gold déplacés de `staging/chicago_taxi/batch=.../` vers `silver/<fenêtre>/batch=.../_gold_candidates/`.
- Fichiers de travail qualité déplacés de `quality/_staging/` vers `silver/<fenêtre>/batch=.../_quality_tmp/` ; les trois datasets opérationnels restent dans `quality/chicago_taxi/`.
- Tests de contrat des dates, chemins et dépendances ajoutés ; guides mis à jour.
- `scripts/verify.py` corrigé : lit le manifeste immuable du batch publié plutôt que de supposer que les dates par défaut `.env` sont celles du dernier run.
- Dremio (6 PDS + 6 vues à configurer), Power BI PBIP (4 pages), Gold stable (3 tables) et Docker Compose conservés.

**Limites inchangées :** `MAX_ROWS=150000` par défaut ; chaque nouvelle période publiée **remplace** les tables métier Gold précédentes ; publication de plusieurs préfixes Parquet **non atomique** ; le PBIP contient des données fictives tant que ses sources M n'ont pas été branchées et contrôlées sur Windows.
