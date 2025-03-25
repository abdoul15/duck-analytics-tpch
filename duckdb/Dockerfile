FROM python:3.10-slim

# Installation des dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    git \
    make \
    gcc \
    g++ \
    postgresql-client \
    rsync \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installation et configuration de TPC-H
WORKDIR /opt/tpch-dbgen
RUN git clone https://github.com/electrum/tpch-dbgen.git . && \
    make clean && \
    make

RUN mkdir -p /opt/tpch-data
RUN ./dbgen -s 0.01 && \
    mv *.tbl /opt/tpch-data/ && \
    chmod 644 /opt/tpch-data/*.tbl && \
    chmod 755 /opt/tpch-data

# Création des répertoires de travail
WORKDIR /app

# Installation des dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Création du répertoire pour les données
RUN mkdir -p /data/iceberg

# Copie des scripts
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Définition des variables d'environnement
ENV PYTHONPATH="/app:${PYTHONPATH}" \
    DUCKDB_DATA_DIR="/data"

# Point d'entrée
ENTRYPOINT ["./entrypoint.sh"]
