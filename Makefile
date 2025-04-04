init-scripts:
	chmod +x postgres/load-tpch.sh

build: init-scripts
	docker compose build duckdb

docker-up:
	docker compose up --build -d

up: build docker-up

down:
	docker compose down -v

rebuild:
	docker compose build
	docker compose up -d

# Exécuter le shell DuckDB
duckdb-shell:
	docker exec -ti duckdb ./entrypoint.sh shell

# Exécuter le pipeline complet avec DuckDB
run-pipeline:
	docker exec -ti duckdb python /app/analytics/run_pipeline.py

# Exécuter uniquement la couche Bronze avec DuckDB
run-duckdb-bronze:
	docker exec -ti duckdb ./entrypoint.sh pipeline bronze

# Exécuter uniquement la couche Silver avec DuckDB
run-duckdb-silver:
	docker exec -ti duckdb ./entrypoint.sh pipeline silver

# Exécuter une commande Python dans le conteneur DuckDB
duckdb-python:
	docker exec -ti duckdb python

# Exécuter une commande bash dans le conteneur DuckDB
duckdb-bash:
	docker exec -ti duckdb bash

# Tests
test:
	docker exec -ti duckdb pytest analytics/tests

# Exécuter les tests unitaires
test-unit:
	docker exec -ti duckdb pytest analytics/tests/unit

# Exécuter les tests d'intégration
test-integration:
	docker exec -ti duckdb pytest analytics/tests/integration

# Exécuter les tests avec couverture de code
test-coverage:
	docker exec -ti duckdb pytest analytics/tests --cov=analytics --cov-report=term-missing --cov-report=xml:coverage.xml
