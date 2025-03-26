import duckdb

def create_finance_dashboard_view_duckdb_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    view_name: str = "finance_dashboard_view"
):
    """
    Crée une vue SQL dans DuckDB à partir d'un DuckDBPyRelation contenant les données
    financières déjà filtrées (ex: via read()).

    Args:
        conn: Connexion DuckDB
        relation: DuckDBPyRelation (ex: dataset.curr_data)
        view_name: Nom de la vue logique à créer
    """

    relation.create_view("tmp_finance_metrics")

    query = f"""
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
    FROM tmp_finance_metrics
    """

    conn.execute(query)
    print(f"✅ Vue `{view_name}` créée à partir du ETLDataSet.")
