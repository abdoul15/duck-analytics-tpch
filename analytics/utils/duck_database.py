import duckdb
import os

def get_table_from_db(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> duckdb.DuckDBPyRelation:
    """
    Récupère une table PostgreSQL entière via DuckDB (sans filtrage).
    
    Args:
        conn: Connexion DuckDB
        table_name: Nom de la table PostgreSQL
    
    Returns:
        DuckDB relation (DuckDBPyRelation)
    """
    # Récupérer les variables d'environnement pour la connexion PostgreSQL
    host = os.getenv('UPS_HOST')
    port = os.getenv('UPS_PORT')
    db = os.getenv('UPS_DATABASE')
    user = os.getenv('UPS_USERNAME')
    password = os.getenv('UPS_PASSWORD')

    required_vars = {
    "UPS_HOST": host,
    "UPS_PORT": port,
    "UPS_DATABASE": db,
    "UPS_USERNAME": user,
    "UPS_PASSWORD": password,
    }

    missing = [k for k, v in required_vars.items() if v is None]
    if missing:
        raise EnvironmentError(f"Variables manquantes: {', '.join(missing)}")

    pg_conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    # Charger le plugin postgres_scanner 
    conn.execute("INSTALL postgres_scanner")
    conn.execute("LOAD postgres_scanner")

    # Extraction du schema et nom de table
    if "." in table_name:
        schema, tbl = table_name.split(".")
    else:
        schema = "public"
        tbl = table_name

    # Requête complète
    query = f"""
    SELECT * FROM postgres_scan('{pg_conn_str}', '{schema}', '{tbl}')
    """

    # Exécuter et retourner la relation
    return conn.from_query(query)
