# Airflow 3 — déclencher une fenêtre d'extraction depuis l'interface

## 1. DAG et tâches

Fichier : `dags/chicago_taxi.py`. Le DAG `chicago_taxi_pipeline` conserve **cinq tâches**, dans cet ordre :

```text
bronze_ingestion
       ↓
silver_processing
       ↓
gold_candidate        (candidats techniques dans Silver)
       ↓
quality_gate          (PASS / FAIL)
       ↓
publish_gold          (uniquement après PASS)
```

Les petits dictionnaires de métadonnées (batch_id, chemins MinIO, comptages) sont transmis par XCom, **jamais les CSV/Parquet**. Les fichiers restent dans MinIO. Spark exécute ses traitements localement dans le conteneur Airflow, et PostgreSQL garde les métadonnées de l'orchestrateur.

## 2. Modifier la période au déclenchement, sans changer le code

Ouvre http://localhost:8080 → `chicago_taxi_pipeline` → **Trigger**. Le formulaire affiche :

| Paramètre | Défaut lu depuis `.env` | Sens |
|---|---|---|
| `extract_start_date` | `START_DATE=2023-01-01` | Début **inclus** |
| `extract_end_date` | `END_DATE=2023-01-08` | Fin **exclue** |

Exemple pour **tout février 2023** : `extract_start_date=2023-02-01`, `extract_end_date=2023-03-01`.

Tu peux aussi fournir le JSON au formulaire de déclenchement :

```json
{
  "extract_start_date": "2023-02-01",
  "extract_end_date": "2023-03-01"
}
```

Airflow valide le format des dates via des `Param` de type `string` / `format=date`. La classe `Settings` valide en plus l'ordre : début strictement antérieur à fin. **Les cinq tâches lisent le même contexte du DagRun** via `_settings_for_run()` ; les valeurs ne sont PAS prises sur `dag.params`, qui ne contiendrait que les défauts. Le champ `start_date` du décorateur `@dag` est une référence de planification, pas la date métier.

`START_DATE` et `END_DATE` de `.env` servent maintenant uniquement de **valeurs par défaut du formulaire** et de valeurs pour les scripts lancés hors Airflow. Le changement de défaut dans `.env` nécessite de recréer/actualiser la configuration des conteneurs, mais **changer la période d'un run dans l'UI ne nécessite aucun rebuild**.

**Attention :** `MAX_ROWS=150000` reste une limite d'échantillon même pour une grande fenêtre. Le Gold est un **snapshot complet de la dernière fenêtre publiée**, pas une accumulation incrémentale des fenêtres précédentes. Les tables qualité/réjections restent historisées par `batch_id`.

## 3. Vérifier le run

Après le déclenchement, vérifie **cinq tâches vertes**, puis :

```powershell
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify.py
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/query_gold.py
```

`scripts/verify.py` recherche le **batch actuellement publié** à partir de `current_publication.json` et de son manifeste immuable. Il affiche la fenêtre réellement publiée, même si tu as choisi une période différente de `.env`. Il ne permet pas, seul, d'affirmer que la dernière tentative Airflow a réussi : contrôle aussi les cinq tâches vertes dans l'UI.

Le dossier candidat du batch est sous `silver/chicago_taxi/start=.../end=.../batch=.../_gold_candidates/`. Le préfixe de la table finale ne change pas : `gold/chicago_taxi/<table>/`. Dremio ne doit promouvoir que les trois tables Gold finales et les trois tables opérationnelles `quality/`, jamais les dossiers Silver internes.

Si un run échoue, examine le log de la tâche en rouge. Relance **un DAG run complet** pour éviter de mélanger les chemins XCom de différents batches ; ne réutilise pas un ancien `publish_gold` sur une nouvelle fenêtre. `max_active_runs=1` évite deux runs concurrents de ce DAG, mais n'assure pas une publication multi-tables atomique.

## 4. Diagnostic

```powershell
docker compose ps -a
docker compose logs -f airflow-scheduler airflow-dag-processor
```

Le projet est configuré pour une démonstration locale uniquement : ports liés à `127.0.0.1`, comptes MinIO démo, pas de SLA/transactions garanties. Ne fais pas `docker compose down -v` si tu souhaites préserver les volumes MinIO, Dremio et PostgreSQL.
