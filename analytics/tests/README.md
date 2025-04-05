# Tests pour Duck Analytics TPCH

Ce répertoire contient les tests unitaires et d'intégration pour le projet Duck Analytics TPCH.

## Structure des tests

```
analytics/tests/
├── conftest.py              # Fixtures partagées pour tous les tests
├── unit/                    # Tests unitaires
│   ├── etl/                 # Tests unitaires pour les classes ETL
│   │   ├── bronze/          # Tests pour les ETL de la couche Bronze
│   │   ├── silver/          # Tests pour les ETL de la couche Silver
│   │   └── gold/            # Tests pour les ETL de la couche Gold
│   └── utils/               # Tests pour les utilitaires
└── integration/             # Tests d'intégration
```

## Exécution des tests

Vous pouvez exécuter les tests en utilisant les commandes suivantes:

### Tous les tests

```bash
make test
```

### Tests unitaires uniquement

```bash
make test-unit
```

### Tests d'intégration uniquement

```bash
make test-integration
```

### Tests avec couverture de code

```bash
make test-coverage
```

## Fixtures

Les fixtures principales sont définies dans `conftest.py`:

- `mock_env_vars`: Mock des variables d'environnement pour la connexion à la base de données
- `mock_duckdb_connection`: Connexion DuckDB en mémoire pour les tests
- `mock_relation`: Relation DuckDB mockée
- `mock_s3_storage`: Mock des opérations de stockage S3

## Ajout de nouveaux tests

### Tests unitaires

Pour ajouter un nouveau test unitaire:

1. Créez un fichier `test_*.py` dans le répertoire approprié sous `unit/`
2. Utilisez les fixtures définies dans `conftest.py`
3. Suivez le modèle des tests existants

Exemple:

```python
import pytest
from unittest.mock import patch, MagicMock
from analytics.etl.bronze.my_module import MyETL

class TestMyETL:
    
    def test_my_function(self, mock_duckdb_connection):
        # Arrange
        etl = MyETL(conn=mock_duckdb_connection)
        
        # Act
        result = etl.my_function()
        
        # Assert
        assert result == expected_value
```

### Tests d'intégration

Pour ajouter un nouveau test d'intégration:

1. Créez un fichier `test_*.py` dans le répertoire `integration/`
2. Utilisez les fixtures définies dans `conftest.py`
3. Testez l'interaction entre plusieurs composants

## Bonnes pratiques

- Utilisez des mocks pour isoler le code testé
- Testez les cas limites et les cas d'erreur
- Maintenez une couverture de code élevée
- Utilisez des assertions claires et descriptives
- Suivez le modèle AAA (Arrange, Act, Assert)
