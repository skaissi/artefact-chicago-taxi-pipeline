# Dremio — brancher les NOUVELLES tables stables

## Démarrage / préservation du compte existant

Le service `dremio` est inclus dans `docker-compose.yml`, volume persistant `dremio_data:/opt/dremio/data`. Si tu as déjà Dremio dans ton Compose, **ne supprime pas ce volume**, ne change pas le nom de projet Compose, ne réinitialise pas l'app. Le contenu du compte et de la source existants ne peut pas être restauré si tu supprimes le volume sans sauvegarde.

```powershell
docker compose up -d dremio
docker compose ps -a
Test-NetConnection localhost -Port 31010
```

UI : http://localhost:9047 ; port client pour Power BI : `localhost:31010`. Dremio peut demander plusieurs minutes au premier démarrage. Si la source `chicago_lake` existe déjà et fonctionne, **garde-la**. Sinon : Add Source → Amazon S3, `Name=chicago_lake`, `AWS Access Key` (identifiants de ton MinIO), désactive encryption pour le HTTP local, Advanced → compatibility mode, connection properties `fs.s3a.path.style.access=true`, `fs.s3a.endpoint=minio:9000` (ne PAS utiliser localhost entre conteneurs, ni http:// dans le paramètre endpoint), et éventuellement allowlist `taxi-datalake`. Jamais de credentials dans le code Git.

## Après un nouveau run complet PASS (cinq tâches)

Navigue dans la source `chicago_lake` → bucket `taxi-datalake` et formate **chacun des six dossiers ci-dessous** en **Parquet** (Format folder). Ce sont des chemins relatifs au bucket :

| Zone | Dossier physique | Nom logique après création des espaces/vues |
|---|---|---|
| Business | `gold/chicago_taxi/daily_revenue/` | `ChicagoTaxi.daily_revenue` |
| Business | `gold/chicago_taxi/daily_trip_metrics/` | `ChicagoTaxi.daily_trip_metrics` |
| Business | `gold/chicago_taxi/top_pickup_zones/` | `ChicagoTaxi.top_pickup_zones` |
| Operations | `quality/chicago_taxi/rejected_records/` | `ChicagoTaxi_Operations.rejected_records` |
| Operations | `quality/chicago_taxi/quality_batch/` | `ChicagoTaxi_Operations.quality_batch` |
| Operations | `quality/chicago_taxi/quality_reasons/` | `ChicagoTaxi_Operations.quality_reasons` |

Les noms logiques ne sont **PAS créés automatiquement** par le simple lancement Docker. Pour les créer : dans Dremio UI crée un **Space** nommé `ChicagoTaxi` et un second `ChicagoTaxi_Operations`. Ouvre `sql/dremio_views.sql`, exécute les six commandes `CREATE OR REPLACE VIEW`, une par une. Si Dremio affiche un nom physique différent dans l'éditeur SQL d'un dataset, copie le nom affiché et adapte la partie `FROM`. Il faut avoir promu le dossier avant de créer une vue pointant dessus. `CREATE OR REPLACE VIEW` dans un Space est documenté par Dremio ; ne le confonds pas avec la promotion automatique d'un fichier.

Test SQL Dremio :

```sql
SELECT * FROM ChicagoTaxi.daily_trip_metrics LIMIT 20;
SELECT reject_reason, reject_type, COUNT(*) AS records
FROM ChicagoTaxi_Operations.rejected_records
GROUP BY reject_reason, reject_type ORDER BY records DESC;
SELECT batch_id, gate_status, reject_rate, duplicate_rate, violations
FROM ChicagoTaxi_Operations.quality_batch
ORDER BY checked_at_utc DESC;
```

`rejected_records` contient des anomalies et doublons **de tous les batches disponibles**, pas seulement la dernière exécution. Filtre par `batch_id` si tu veux te concentrer sur un run précis. Les 3 tables métier Gold ne comportent aucune plage `start/end` dans leur chemin ; elles représentent uniquement la dernière fenêtre exécutée et publiée. L'ancien dossier Gold daté reste dans MinIO jusqu'au nettoyage optionnel post-migration.

**Permissions** : les deux Spaces séparent la navigation à des fins de démonstration ; ils n'instaurent PAS une isolation des droits puisque la source MinIO est configurée avec des credentials root. Pour un véritable data mesh, utiliser des comptes MinIO IAM/policies distincts, RLS/CLS si licence et capacités adaptées, une gouvernance/catalogue certifié et des revues d'accès.

## Après les runs suivants — rafraîchir les métadonnées Parquet

Comme l'écriture Gold V2 réécrit les fichiers Parquet d'un dossier stable (et l'historique qualité ajoute des fichiers), une vue Dremio peut afficher des métadonnées en cache jusqu'à la prochaine actualisation. Si une requête garde l'ancien total ou indique qu'un ancien fichier est introuvable, exécute la commande sur les **datasets PHYSIQUES** concernés (non pas uniquement sur la vue) :

```sql
ALTER TABLE chicago_lake."taxi-datalake".gold.chicago_taxi.daily_revenue REFRESH METADATA FORCE UPDATE;
ALTER TABLE chicago_lake."taxi-datalake".gold.chicago_taxi.daily_trip_metrics REFRESH METADATA FORCE UPDATE;
ALTER TABLE chicago_lake."taxi-datalake".gold.chicago_taxi.top_pickup_zones REFRESH METADATA FORCE UPDATE;
ALTER TABLE chicago_lake."taxi-datalake".quality.chicago_taxi.rejected_records REFRESH METADATA FORCE UPDATE;
ALTER TABLE chicago_lake."taxi-datalake".quality.chicago_taxi.quality_batch REFRESH METADATA FORCE UPDATE;
ALTER TABLE chicago_lake."taxi-datalake".quality.chicago_taxi.quality_reasons REFRESH METADATA FORCE UPDATE;
```

Les commandes ci-dessus sont des templates pour les chemins exacts configurés dans ce ZIP : adapte les identifiants en cas de nom de source personnalisé. Rafraîchis ensuite le rapport Power BI Desktop. Pour la version de Dremio où `FORCE UPDATE` serait refusé, commence par `ALTER TABLE <PDS> REFRESH METADATA;` et vérifie la documentation de ta version. Cette étape n'est pas automatisée dans le DAG (Dremio n'est pas un prérequis de production des fichiers).
