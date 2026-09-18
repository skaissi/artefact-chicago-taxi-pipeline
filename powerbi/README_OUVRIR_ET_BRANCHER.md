# Power BI FRONT — 4 pages (previsualisation fictive)

Ouvrir `ChicagoTaxi.pbip` dans Power BI Desktop recent. Source M = DEMO fictive, non connectee a Dremio.
Pages : Executive Overview, Pickup Zones, Data Quality, Rejection Explorer.
Les datasets sont maintenant produits DIRECTEMENT par le nouveau DAG Airflow V2 : aucun script manuel publish_quality_for_bi.py.
Connexion 6 vues Dremio, schema, limites et etapes de verification : `../docs/POWERBI_CONNECTION_FR.md`.
Validation effectuee ici = fichiers JSON/TMDL statiques seulement ; Power BI Desktop Windows non disponible.
Retirer les etiquettes DEMO uniquement apres avoir branche les 6 tables et verifie les chiffres reels.
