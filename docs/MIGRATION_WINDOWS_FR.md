# MIGRATION DE TON INSTALLATION EXISTANTE — Windows / PowerShell

> Ne supprime **ni** les volumes Docker, **ni** ton dossier original tant que le remplacement n'est pas validé. **Ne lance jamais `docker compose down -v`**, `docker system prune --volumes`, `Reset to factory defaults` ou `wsl --unregister` pour cette migration.

## Étape A — PRÉSERVER les volumes de ton projet actuel

1. Arrête de déclencher le DAG; attends la fin de toute exécution et ferme Power BI Desktop (sinon le PBIP risque de contenir des modifications en cours).
2. Place-toi dans **ton dossier actuel** (avec guillemets car `artefact case study` contient des espaces) :

```powershell
cd "C:\Users\soufi\OneDrive\Desktop\artefact case study\artefact-chicago-taxi-airflow\artefact-chicago-taxi-airflow"
```

3. Sauvegarde le projet et les réglages avant de remplacer les fichiers. Dans le dossier courant :

```powershell
$backup = Join-Path (Split-Path (Get-Location) -Parent) ("backup-before-v2-" + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup | Out-Null
Copy-Item .\docker-compose.yml, .\.env -Destination $backup
Copy-Item .\dags, .\src, .\scripts, .\docs -Destination $backup -Recurse
# N'oublie pas de sauvegarder séparément ton ancien rapport PBIP si tu l'as modifié.
docker compose config --services
```

4. **Sauvegarde aussi les données** si tu tiens aux anciens historiques Airflow/Dremio :

```powershell
docker compose exec -T postgres pg_dump -U airflow -d airflow --format=custom --file=/tmp/airflow-backup.dump
docker compose cp postgres:/tmp/airflow-backup.dump "$backup\airflow-backup.dump"
# Pour MinIO : copie locale (répertoire /data). Vérifie l'espace disque disponible.
docker compose cp minio:/data "$backup\minio-data"
# Si Dremio EXISTAIT déjà dans ton ancien Compose et qu'il est démarré :
docker compose cp dremio:/opt/dremio/data "$backup\dremio-data"
```

Attention : `docker compose cp` depuis une base active ne garantit pas une sauvegarde transactionnelle de son volume. Utilise `pg_dump` pour PostgreSQL et, si un backup Dremio parfaitement cohérent est indispensable, arrête les services avant une copie à froid du volume. Le projet conserve les volumes par défaut si tu remplaces les fichiers **dans le même dossier, avec le même nom de projet Compose**. Ce guide n'effectue aucune suppression automatique.

## Étape B — Remplacer les FICHIERS, pas les volumes

1. Télécharge et décompresse `Artefact_Chicago_Taxi_V2_Complet.zip` dans **un dossier temporaire**.
2. Entre dans son sous-dossier `artefact-chicago-taxi-airflow` : c'est ce dossier qui contient `docker-compose.yml`.
3. Copie ses **fichiers et sous-dossiers** vers le dossier actuel (par-dessus les fichiers du code). **Ne copie pas le `.env` de la nouvelle archive**, car seul `.env.example` est fourni : conserve ton `.env` et tes identifiants. Voici une méthode PowerShell en adaptant seulement `$source` au dossier décompressé :

```powershell
$source = "C:\CHEMIN\VERS\ZIP_DECOMPRESSE\artefact-chicago-taxi-airflow"
$target = "C:\Users\soufi\OneDrive\Desktop\artefact case study\artefact-chicago-taxi-airflow\artefact-chicago-taxi-airflow"
Copy-Item -Path "$source\*" -Destination $target -Recurse -Force
# -Force inclut aussi les fichiers cachés ; aucun .env n'existe dans le ZIP.
cd $target
```

**Ne change pas de dossier Compose** lors de ce remplacement : déplacer vers un autre répertoire peut modifier le préfixe des volumes et te donner l'impression que les données ont disparu. Si tu as déjà un service Dremio personnalisé dans Compose, garde les anciens `docker-compose.yml` sauvegardés, vérifie les ports et adapte le service avant de démarrer. Le service Dremio fourni reprend `dremio_data:/opt/dremio/data`, `9047` et `31010`.

4. Ajoute les nouvelles variables qualité à ton `.env` (ne duplique pas les variables existantes) :

```dotenv
MAX_REJECT_RATE=0.10
MAX_DUPLICATE_RATE=0.10
MAX_EXTRACT_AGE_HOURS=24
MAX_TRIP_MILES=500
MAX_TRIP_DURATION_SECONDS=86400
MAX_TRIP_TOTAL_USD=5000
```

**Seuils de démonstration, pas des engagements contractuels** ; les seuils doivent être approuvés sur des données métier et ajustés si nécessaire. Le plafond `MAX_ROWS` de 150 000 reste en place.

## Étape C — Relancer sans effacer les données

Dans **le dossier existant** :

```powershell
docker compose config --services
docker compose up --build -d
docker compose ps -a
```

Tu dois voir `postgres`, `minio`, `airflow-init` (`Exited (0)` normal), `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `dremio`. L'interface Dremio peut être longue à initialiser.

Ouvre Airflow sur http://localhost:8080, désactive/ignore les anciens runs historiques et lance un **nouveau run complet** de `chicago_taxi_pipeline`. Le graphe doit afficher CINQ tâches vertes : `bronze_ingestion → silver_processing → gold_candidate → quality_gate → publish_gold`.

Vérifie la publication :

```powershell
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify.py
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/query_gold.py
```

La commande `verify.py` exige que le **dernier batch** soit validé et publié, pas seulement que le Gold précédent existe encore.

## Étape D — Dremio / Power BI

Suis `docs/DREMIO_SETUP_FR.md`, puis `docs/POWERBI_CONNECTION_FR.md`. L'ancien dossier Gold daté et les anciens datasets Dremio restent présents **jusqu'au nettoyage volontaire** ; les nouvelles tables stables sont `gold/chicago_taxi/<table>` sans plage de dates. Les datasets qualité sont sous `quality/chicago_taxi/*` (hors Gold métier).

### Nettoyage optionnel de l'ancien Gold, UNIQUEMENT après validation

```powershell
# Prévisualiser d'abord les objets concernés (aucune suppression) :
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/archive_legacy_gold.py
# Si la nouvelle version et Dremio fonctionnent et que tu souhaites réellement archiver l'ancien préfixe :
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/archive_legacy_gold.py --apply
```

Ce script sauvegarde **chaque objet** `gold/chicago_taxi/start=...` sous `archive/legacy_gold/chicago_taxi/start=...`, compare les tailles, puis supprime uniquement l'objet source. Il ne touche pas Bronze, Silver, le nouveau Gold, PostgreSQL ni tes volumes. Les promotions d'anciens datasets Dremio doivent être retirées manuellement ensuite.

### Retour arrière

Ne lance pas `down -v`. Remets les fichiers du dossier `$backup` à la place des nouveaux fichiers ; conserve les volumes. Attention : les nouveaux runs ne seront alors pas gérés par l'ancienne version et les anciens chemins Gold peuvent avoir été archivés volontairement. N'applique donc le script de nettoyage que quand tu renonces réellement à l'ancien layout.
