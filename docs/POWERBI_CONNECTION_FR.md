# Power BI — brancher les 4 pages sur Dremio

## Important avant tout

Le PBIP `powerbi/ChicagoTaxi.pbip` contient un **modèle de prévisualisation aux chiffres entièrement fictifs**. Les visuels sont des fichiers PBIR natifs ; la connexion n'est PAS préconfigurée sur ton compte Dremio (elle nécessiterait tes credentials), et l'ouverture dans Windows Desktop n'a PAS pu être testée ici. Le modèle fictif ne doit pas être présenté comme résultat réel.

## Connexion

1. Vérifie Dremio dans PowerShell : `Test-NetConnection localhost -Port 31010` et ouvre http://localhost:9047. **La source MinIO doit déjà être branchée et les 6 dossiers promus**, et les vues `ChicagoTaxi` et `ChicagoTaxi_Operations` créées (guide Dremio).
2. Power BI Desktop → Get Data → **Dremio Software** (pas Dremio Cloud). Server `localhost:31010`, Encryption `Disabled` pour la démo HTTP locale, mode **Import** pour démarrer. Authenticate Basic avec ton **compte Dremio** (pas MinIO). Dans les versions récentes, ADBC est optionnel en préversion et utilise `adbc://...` ; ne mélange pas ce mode avec la première connexion classique par port 31010.
3. Navigator → sélectionner les six vues (ou six PDS), noter le détail exact des étapes M produites par Power BI. Le rapport existant doit garder les noms *internes* ci-dessous. Soit remplace `Source` dans les cinq/six partitions M, soit dans Power Query copie le M « source Dremio » de la nouvelle connexion et colle-le dans les requêtes existantes, puis supprime les nouvelles requêtes dupliquées. **Ne renomme pas les tables internes**, sinon les visuels cassent.

| Nom du modèle PBIP | Vue Dremio | Colonnes à préserver |
|---|---|---|
| `Fact_Daily_Revenue` | `ChicagoTaxi.daily_revenue` | trip_date, trip_count, total_revenue_usd |
| `Fact_Daily_Metrics` | `ChicagoTaxi.daily_trip_metrics` | trip_date, trip_count, avg_duration_min, avg_trip_miles |
| `Fact_Top_Zones` | `ChicagoTaxi.top_pickup_zones` | rank, pickup_zone, trip_count, total_revenue_usd |
| `Fact_Quality_Batch` | `ChicagoTaxi_Operations.quality_batch` | batch_id, recorded_at_utc, checked_at_utc, gate_status, raw_rows, clean_rows, rejected_rows, duplicate_valid_rows, reconciliation_delta, gold_daily_rows, gold_top_zones_rows + seuils |
| `Fact_Quality_Reasons` | `ChicagoTaxi_Operations.quality_reasons` | batch_id, reject_reason, reject_count |
| `Fact_Rejected_Records` | `ChicagoTaxi_Operations.rejected_records` | batch_id, reject_type, reject_reason, trip_id, trip_date, trip_total_usd, trip_miles, duration_seconds, pickup_community_area |

`DimDate` est une table calculée depuis `Fact_Daily_Revenue`, pas une sixième source Dremio.

4. Appliquer / Actualiser, contrôler totaux d'un batch Dremio = mesures DAX. Sur Data Quality et Rejection Explorer, **filtre un batch_id** lorsque tu veux auditer une exécution ; les datasets qualité sont historisés. Les lignes invalides et les doublons sont comptés séparément dans `quality_batch`; la somme des raisons compte les deux.
5. Enlève les mentions `DEMO` **uniquement lorsque les six tables sont reliées à Dremio ET les chiffres vérifiés**. Enregistre en `.pbix` depuis Power BI Desktop si tu souhaites un PBIX.

Dépannage : si une colonne apparaît en texte ou type incorrect, force ses types après la navigation. Power BI Service ne peut pas joindre `localhost` de ton ordinateur à distance : configurer une passerelle et une adresse réseau stable pour refresh dans le service. Aucune actualisation cloud automatique n'est incluse dans ce projet.
