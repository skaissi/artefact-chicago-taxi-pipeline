# Installer la V2.1 (Windows PowerShell) — garder les données existantes

## A. Si tu possèdes déjà la V2 en fonctionnement

1. Sauvegarde tes modifications personnelles (`.env`, `docker-compose.yml`, DAG et scripts modifiés). **Ne remplace pas ton `.env`** : il n'est pas contenu dans cette archive.
2. Décompresse `Artefact_Chicago_Taxi_V2_1_Complet.zip` dans un autre dossier, puis ouvre ce dossier.
3. Si ton dossier V2 contient `docker-compose.yml`, garde **le même répertoire de projet et le même nom de projet Compose** afin de retrouver les mêmes volumes. Depuis le répertoire extrait, recopie les nouveaux `dags/`, `src/`, `tests/`, `docs/`, `scripts/`, `README.md`, `.env.example` et le reste des fichiers du ZIP vers le répertoire V2. Ne copie pas un `.env` externe par-dessus ton `.env`.
4. Depuis ton répertoire V2 :

```powershell
docker compose config --services
docker compose up --build -d
docker compose ps -a
```

5. Ne lance **jamais** `docker compose down -v` si tu souhaites conserver MinIO, Dremio et PostgreSQL.
6. Attends l'actualisation du DAG dans l'UI : http://localhost:8080 → `chicago_taxi_pipeline` → Trigger → modifie les dates.

**Important :** la V2.1 déplace les *futurs candidats* sous Silver. Les anciens objets `staging/` restent éventuellement dans MinIO ; ne les supprime pas avant d'avoir vérifié le dernier run et la publication. Les anciens rapports d'audit peuvent référencer d'anciens candidats utilisés pour un rollback.

## B. Si tu pars d'un projet propre / dossier vide

Décompresse le ZIP, entre dans le dossier **qui contient `docker-compose.yml`**, puis :

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps -a
```

Si les ports 8080, 9000, 9001, 9047 ou 31010 sont occupés par d'anciens conteneurs, arrête les conteneurs concurrents sans supprimer leurs volumes. Le projet démo fonctionne en local uniquement.

## C. Choisir la période au lancement

Dans Airflow : http://localhost:8080 → `chicago_taxi_pipeline` → **Trigger**.

- `extract_start_date` = `2023-01-01` par défaut, **incluse**.
- `extract_end_date` = `2023-01-08` par défaut, **exclue**.

Exemple pour mars 2023 : début `2023-03-01` et fin `2023-04-01`. Les valeurs `.env` restent les défauts ; inutile de changer le code ou reconstruire Docker pour lancer une autre période.

Après cinq tâches vertes, contrôle :

```powershell
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify.py
```

Ce script affiche l'identifiant de batch et la fenêtre **réellement publiée** ; il ne suppose plus que le formulaire Airflow utilise les dates du `.env`.

**Rappels :** `MAX_ROWS=150000` plafonne le volume, et les trois tables Gold stables représentent **seulement la dernière fenêtre validée et publiée**. Un run sur mars après janvier remplacera janvier dans Gold. Les datasets `quality/` et rejets restent historisés.

## D. Dremio + Power BI : rien à recréer si l'ancien modèle est déjà connecté

Dremio : http://localhost:9047 ; SQL : `localhost:31010`. Les **six dossiers physiques restent inchangés** :

- Métier : `gold/chicago_taxi/{daily_revenue,daily_trip_metrics,top_pickup_zones}`.
- Opérations : `quality/chicago_taxi/{rejected_records,quality_batch,quality_reasons}`.

Si nouvelle installation, suis `docs/DREMIO_SETUP_FR.md` et crée les Spaces/vues. Le rapport PBIP reste dans `powerbi/ChicagoTaxi.pbip` avec **quatre pages** ; guide de connexion dans `docs/POWERBI_CONNECTION_FR.md`. Le rapport contient des données fictives tant que tu ne l'as pas relié à Dremio et vérifié dans Power BI Desktop.

## E. Problèmes habituels

- `open //./pipe/dockerDesktopLinuxEngine`: lancer Docker Desktop, vérifier `docker info` (Client **et** Server), puis relancer `docker compose up --build -d`.
- Nouveau formulaire Airflow absent : rafraîchir l'interface après le rechargement du DAG processor ; vérifier les logs et l'absence d'erreur d'import.
- Dremio ne trouve pas un ancien fichier Gold : rafraîchir les métadonnées des PDS (guide Dremio).
- Power BI SSL 1150 : pour le Dremio HTTP local, choisir Encryption **Disabled** dans la connexion. Les signatures M varient selon les versions du connecteur : reconstruis l'étape Source à partir de l'assistant si la tienne refuse un nombre d'arguments donné ; ne copie pas aveuglément une expression à cinq arguments.
