# DuckDB TPC-H Analytics Pipeline with AWS S3

## Introduction

Ce projet implémente un **pipeline analytique complet** basé sur les données **TPC-H**, utilisant **DuckDB** comme moteur de traitement et **AWS S3** comme stockage. L'objectif est de produire des métriques business exploitables dans des outils BI.

## Architecture du Pipeline

![Architecture du Pipeline](docs/architecture.png)

### 1. Ingestion (Bronze Layer)
- Extraction depuis PostgreSQL via DuckDB
- Chargement initial en mémoire
- Sauvegarde des données brutes sur AWS S3 (format Parquet)

### 2. Transformation (Silver Layer)
- Modélisation dimensionnelle (faits/dimensions)
- Nettoyage et normalisation
- Stockage intermédiaire sur S3

### 3. Agrégation (Gold Layer)
- Calcul des indicateurs clés:
  - **Finance**: Revenus, marges, taxes
  - **Marketing**: Segmentation client
- Export vers S3 pour consommation BI

### 4. Interface
- Vues métier matérialisées
- Compatible avec AWS Athena/Redshift

## Stack Technique

| Composant       | Usage                     |
|-----------------|---------------------------|
| DuckDB          | Traitement ETL in-memory  |
| PostgreSQL      | Source TPC-H              |
| AWS S3          | Stockage des données      |
| Docker          | Environnement             |
| Python          | Implémentation            |

## Configuration AWS

### 1. Création du bucket S3

1. Connectez-vous à la console AWS (https://aws.amazon.com/)
2. Allez dans le service S3
3. Cliquez sur "Créer un bucket"
4. Donnez un nom unique à votre bucket (ex: `duckdb-tpch-analytics`)
5. Sélectionnez la région souhaitée (ex: `eu-west-1`)
6. Configurez les options selon vos besoins (versionning, chiffrement, etc.)
7. Cliquez sur "Créer un bucket"

### 2. Configuration des credentials

1. Allez dans IAM > Utilisateurs
2. Créez un utilisateur avec accès programmatique
3. Attachez la politique `AmazonS3FullAccess` (ou une politique plus restrictive)
4. Notez les credentials (Access Key ID et Secret Access Key)

### 3. Variables d'environnement
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=eu-west-1
S3_BUCKET=your-bucket-name
S3_DATA_PATH=/analytics/tpch
```

### Initialisation DuckDB avec S3

**Note de sécurité** :  
Pour des environnements de production, utilisez des rôles IAM plutôt que des credentials statiques, et restreignez les permissions au minimum nécessaire.
```python
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute(f"""
    SET s3_region='{AWS_REGION}';
    SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
    SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
""")
```

## Démarrage Rapide

### 1. Installation
```bash
git clone https://github.com/votre-repo/duck-analytics-tpch.git
cd duck-analytics-tpch
```

### 2. Configuration
```bash
cp .env.example .env
# Remplir les credentials AWS
```

### 3. Exécution
```bash
make up  # Démarrer les containers
make run-pipeline  # Lancer le pipeline
```

## Accès aux Données

### Via AWS Services
- **Athena**: Interroger directement les données Parquet sur S3
- **Redshift Spectrum**: Créer des tables externes pointant vers S3
- **QuickSight**: Connecteur S3 direct

### Exemple de requête Athena
```sql
SELECT * FROM parquet_scan('s3://your-bucket/analytics/tpch/gold/finance_metrics/*')
```

## Évolution

- Intégration Glue Catalog
- Orchestration Step Functions
- Monitoring CloudWatch
- Sécurité IAM fine
