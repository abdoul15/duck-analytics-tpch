import duckdb
import logging
import os
from dotenv import load_dotenv 

from analytics.utils.logging_config import setup_logging
from analytics.etl.gold.finance_metrics import FinanceMetricsGold
from analytics.etl.gold.marketing_metrics import MarketingMetricsGold
from analytics.etl.interface.finance_views import create_finance_dashboard_view_duckdb_from_dataset
from analytics.etl.interface.marketing_views import create_marketing_dashboard_view_duckdb_from_dataset



if __name__ == "__main__":
    # Charger les variables d'environnement depuis .env
    load_dotenv()

    # Configuration des logs
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("--- Démarrage du pipeline ETL ---")

    try:
        
        s3_bucket = os.getenv("S3_BUCKET")
        print(s3_bucket)

        if not all([s3_bucket]):
            logger.error("Variables d'environnement AWS manquantes (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET)")
            raise ValueError("Configuration AWS incomplète.")


        s3_base_path = f"s3://{s3_bucket}/interface"
        finance_export_path = f"{s3_base_path}/finance/finance_dashboard.parquet"
        marketing_export_path = f"{s3_base_path}/marketing/marketing_dashboard.parquet"

        # 2. Connexion DuckDB avec plugins utiles
        logger.debug("Connexion à DuckDB")
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

        

        # --- Finance ---
        logger.info("Exécution du ETL FinanceMetrics (Gold)")
        finance_etl = FinanceMetricsGold(conn) 
        finance_etl.run()
        finance_data = finance_etl.read() 
        logger.info("Création/Export de la vue Finance")
        create_finance_dashboard_view_duckdb_from_dataset(
            conn=conn,
            relation=finance_data.curr_data,
            s3_export_path=finance_export_path
        )

        # --- Marketing ---
        logger.info("Exécution du ETL MarketingMetrics (Gold)")
        marketing_etl = MarketingMetricsGold(conn) 
        marketing_etl.run()
        marketing_data = marketing_etl.read() 
        logger.info("Création/Export de la vue Marketing")
        create_marketing_dashboard_view_duckdb_from_dataset(
            conn=conn,
            relation=marketing_data.curr_data,
            s3_export_path=marketing_export_path
        )

        logger.info("--- Pipeline ETL terminé avec succès ---")

    except Exception as e:
        logger.exception(f"Erreur lors de l'exécution du pipeline: {str(e)}") 
        raise
