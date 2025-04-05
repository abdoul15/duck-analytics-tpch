from contextlib import contextmanager

@contextmanager
def with_temp_view(conn, relation, view_name: str):
    """
    Context manager pour créer une vue temporaire et la supprimer après.
    """
    relation.create_view(view_name, replace=True)
    try:
        yield view_name
    finally:
        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
