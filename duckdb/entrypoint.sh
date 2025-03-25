#!/bin/bash
set -e

# Fonction pour attendre que PostgreSQL soit prêt
wait_for_postgres() {
  echo "Attente de PostgreSQL..."
  until PGPASSWORD=$UPS_PASSWORD psql -h $UPS_HOST -U $UPS_USERNAME -d $UPS_DATABASE -c '\q'; do
    echo "PostgreSQL n'est pas encore prêt - attente..."
    sleep 2
  done
  echo "PostgreSQL est prêt!"
}

# Attendre que PostgreSQL soit prêt
wait_for_postgres


# Exécuter la commande spécifiée ou lancer un shell interactif
if [ "$1" = "pipeline" ]; then
  echo "Exécution du pipeline DuckDB..."
  python /app/analytics/run_pipeline.py "${@:2}"
elif [ "$1" = "shell" ]; then
  echo "Lancement du shell DuckDB..."
  python -c "import duckdb; conn = duckdb.connect(); print('DuckDB shell interactif lancé. Utilisez conn.execute(\"VOTRE REQUÊTE SQL\") pour exécuter des requêtes.'); import code; code.interact(local=locals())"
else
  echo "Commande non reconnue. Utilisation: entrypoint.sh [pipeline|shell] [args...]"
  echo "  pipeline: Exécute le pipeline ETL DuckDB"
  echo "  shell: Lance un shell DuckDB interactif"
  echo "  Si aucune commande n'est spécifiée, un shell bash est lancé"
  exec "$@"
fi
