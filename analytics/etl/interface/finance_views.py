import duckdb
from datetime import date
import logging # Importer le module logging

# Obtenir une instance de logger
logger = logging.getLogger(__name__)

def create_finance_dashboard_view_duckdb_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    s3_export_path: str, # Nouveau paramètre pour le chemin S3
    view_name: str = "finance_dashboard_view"
):
    """
    Crée une vue SQL dans DuckDB et exporte les données vers S3 au format Parquet
    à partir d'un DuckDBPyRelation contenant les données financières.

    Args:
        conn: Connexion DuckDB
        relation: DuckDBPyRelation (ex: dataset.curr_data)
        s3_export_path: Chemin S3 pour l'export (ex: s3://bucket/interface/finance/finance_dashboard.parquet)
        view_name: Nom de la vue logique à créer dans DuckDB
    """

    # 1. Créer une vue temporaire pour l'export et la vue finale (utilise la relation originale)
    relation.create_view("tmp_finance_metrics_for_export", replace=True)

    # 2. Exporter vers S3 (sans partitionnement)
    # Assurez-vous que s3_export_path inclut le nom du fichier, ex: s3://.../finance_dashboard.parquet
    export_query = f"""
    COPY tmp_finance_metrics_for_export
    TO '{s3_export_path}'
    (FORMAT 'parquet', OVERWRITE_OR_IGNORE);
    """
    try:
        conn.execute(export_query)
        # Remplacer print par logger.info
        logger.info(f"Données de la vue finance exportées vers {s3_export_path}")
    except Exception as e:
        # Remplacer print par logger.error
        logger.error(f"Erreur lors de l'export S3 pour la vue finance: {e}")
        # On continue quand même pour créer la vue locale

    # 3. Créer la vue dans DuckDB (utilise la même vue temporaire)
    view_query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        date AS date,
        customer_nation AS pays_client,
        customer_region AS region_client,
        total_revenue AS revenu_total,
        total_tax AS taxes_totales,
        total_discounts AS remises_totales,
        accounts_receivable AS creances_clients,
        estimated_margin AS marge_estimee,
        margin_percentage AS taux_marge_pct,
        order_count AS nombre_commandes,
        average_order_value AS valeur_moyenne_commande,
        avg_open_order_age AS age_moyen_commandes_jours
    FROM tmp_finance_metrics_for_export -- Utiliser la vue temporaire
    """

    try:
        conn.execute(view_query)
        # Remplacer print par logger.info
        logger.info(f"Vue DuckDB `{view_name}` créée.")
    except Exception as e:
        # Remplacer print par logger.error
        logger.error(f"Erreur lors de la création de la vue DuckDB `{view_name}`: {e}")

    # Nettoyage de la vue temporaire
    conn.execute("DROP VIEW IF EXISTS tmp_finance_metrics_for_export;")
