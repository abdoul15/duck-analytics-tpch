import duckdb
from analytics.utils.logging_config import setup_logging
from analytics.etl.bronze.customer import CustomerBronze
from analytics.etl.silver.dim_customer import DimCustomerSilver
from analytics.etl.silver.dim_part import DimPartSilver
from analytics.etl.gold.finance_metrics import FinanceMetricsGold
from analytics.etl.gold.marketing_metrics import MarketingMetricsGold
from analytics.etl.bronze.orders import OrdersBronze
import logging



if __name__ == "__main__":
    # Configuration des logs
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Démarrage du pipeline ETL")

    try:
        # 1. Connexion DuckDB avec plugins utiles
        logger.debug("Connexion à DuckDB")
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

        logger.info("Exécution du ETL FinanceMetrics")
        dim = FinanceMetricsGold(conn=conn, load_data=False)
        dim.run()
        dim_data = dim.read()
        dim_data.curr_data.show()

        logger.info("Exécution du ETL MarketingMetrics")
        dim_m = MarketingMetricsGold(conn=conn,load_data=False)
        dim_m.run()
        dim_m_data=dim_m.read()
        dim_m_data.curr_data.show()

        logger.info("✅ ETL Execution terminé avec succès")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline: {str(e)}")
        raise
