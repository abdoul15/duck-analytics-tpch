import duckdb


def create_marketing_dashboard_view_duckdb_from_dataset(
    conn: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    view_name: str = "marketing_dashboard_view"
):
    """
    Crée une vue SQL DuckDB pour exposer les métriques marketing lisibles par un outil BI.

    Args:
        conn: Connexion DuckDB
        relation: DuckDBPyRelation contenant les métriques marketing (déjà filtrées)
        view_name: Nom de la vue logique à créer
    """
    
    relation.create_view("tmp_marketing_metrics", replace=True)

    query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        month AS mois,
        market_segment AS segment_marche,
        total_revenue AS revenu_total,
        avg_order_value AS valeur_moyenne_commande,
        order_count AS nombre_commandes,
        late_delivery_rate AS taux_retard_livraison_pct,
        top_brand AS marque_preferee
    FROM tmp_marketing_metrics
    """

    conn.execute(query)
    print(f"✅ Vue `{view_name}` créée pour le département Marketing.")
