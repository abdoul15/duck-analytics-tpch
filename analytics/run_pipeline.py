import duckdb
from analytics.etl.bronze.customer import CustomerBronzeETL
from analytics.etl.silver.dim_customer import DimCustomerSilverETL
from analytics.etl.silver.dim_part import DimPartSilverETL
from analytics.etl.gold.finance_metrics import FinanceMetricsGoldETL
from analytics.etl.gold.marketing_metrics import MarketingMetricsGoldETL



if __name__ == "__main__":
    # 1. Connexion DuckDB avec plugins utiles
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

    # # 2. Lancer l'ETL
    # etl = CustomerBronzeETL(conn=conn)
    # etl.run()

    # # Lire tout le répertoire S3
    # dataset = etl.read()

    # # Afficher les données
    # dataset.curr_data.show()

    dim = FinanceMetricsGoldETL(conn=conn)
    dim.run()
    dim_data = dim.read()

    dim_data.curr_data.show()

    dim_m = MarketingMetricsGoldETL(conn=conn)
    dim_m.run()
    dim_data_m = dim_m.read()

    dim_data_m.curr_data.show()

    

    print("✅ ETL Execution terminé.")
