import duckdb
from analytics.etl.bronze.customer import CustomerBronzeETL

if __name__ == "__main__":
    # 1. Connexion DuckDB avec plugins utiles
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

    # 2. Lancer l'ETL
    etl = CustomerBronzeETL(conn=conn)
    etl.run()

    # Lire tout le répertoire S3
    dataset = etl.read()

    # Afficher les données
    dataset.curr_data.show()

    

    print("✅ ETL Bronze terminé.")
