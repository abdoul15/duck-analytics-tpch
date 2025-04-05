import duckdb
import logging # Importer le module logging

# Obtenir une instance de logger
logger = logging.getLogger(__name__)

def create_marketing_dashboard_view_duckdb_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    s3_export_path: str, # Nouveau paramètre pour le chemin S3
    view_name: str = "marketing_dashboard_view"
):
    """
    Crée une vue SQL DuckDB et exporte les données vers S3 au format Parquet
    pour exposer les métriques marketing.

    Args:
        conn: Connexion DuckDB
        relation: DuckDBPyRelation contenant les métriques marketing (déjà filtrées)
        s3_export_path: Chemin S3 pour l'export (ex: s3://bucket/interface/marketing/marketing_dashboard.parquet)
        view_name: Nom de la vue logique à créer dans DuckDB
    """
    
    # 1. Créer une vue temporaire pour l'export et la vue finale
    relation.create_view("tmp_marketing_metrics_for_export", replace=True)

    # 2. Exporter vers S3 (sans partitionnement)
    # Assurez-vous que s3_export_path inclut le nom du fichier, ex: s3://.../marketing_dashboard.parquet
    export_query = f"""
    COPY tmp_marketing_metrics_for_export
    TO '{s3_export_path}'
    (FORMAT 'parquet', OVERWRITE_OR_IGNORE);
    """
    try:
        conn.execute(export_query)
        # Remplacer print par logger.info
        logger.info(f"Données de la vue marketing exportées vers {s3_export_path}")
    except Exception as e:
        # Remplacer print par logger.error
        logger.error(f"Erreur lors de l'export S3 pour la vue marketing: {e}")
        # On continue quand même pour créer la vue locale

    # 3. Créer la vue dans DuckDB
    view_query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        month AS mois,
        market_segment AS segment_marche,
        total_revenue AS revenu_total,
        avg_order_value AS valeur_moyenne_commande,
        order_count AS nombre_commandes,
        late_delivery_rate AS taux_retard_livraison_pct,
        top_brand AS marque_preferee
    FROM tmp_marketing_metrics_for_export -- Utiliser la vue temporaire
    """

    try:
        conn.execute(view_query)
        # Remplacer print par logger.info
        logger.info(f"Vue DuckDB `{view_name}` créée pour le département Marketing.")
    except Exception as e:
        # Remplacer print par logger.error
        logger.error(f"Erreur lors de la création de la vue DuckDB `{view_name}`: {e}")

    # Nettoyage de la vue temporaire
    conn.execute("DROP VIEW IF EXISTS tmp_marketing_metrics_for_export;")
