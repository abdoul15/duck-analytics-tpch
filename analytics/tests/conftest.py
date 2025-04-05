import os
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_env_vars():
    """Fixture to mock environment variables for database connection."""
    with patch.dict(os.environ, {
        'UPS_HOST': 'localhost',
        'UPS_PORT': '5432',
        'UPS_DATABASE': 'test_db',
        'UPS_USERNAME': 'test_user',
        'UPS_PASSWORD': 'test_password'
    }):
        yield


@pytest.fixture
def mock_duckdb_connection():
    """Fixture to create a mock DuckDB connection."""
    # Create a fully mocked DuckDB connection
    conn = MagicMock()
    
    # Mock the execute method
    conn.execute.return_value = conn
    
    # Mock the table method to return a mock relation
    mock_relation = MagicMock()
    conn.table.return_value = mock_relation
    
    # Mock the from_query method to return a mock relation
    conn.from_query.return_value = mock_relation
    
    # Mock the read_parquet method to return a mock relation
    conn.read_parquet.return_value = mock_relation
    
    # Set up a mock result for execute().fetchone()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = ['2023-01-01T12:00:00']
    conn.execute.return_value = mock_result
    
    return conn


@pytest.fixture
def mock_relation():
    """Fixture to create a mock DuckDB relation."""
    relation = MagicMock()
    
    # Mock the create_view method
    relation.create_view.return_value = None
    
    # Mock the df method to return a mock pandas DataFrame
    relation.df.return_value = "mock_pandas_df"
    
    # Mock the show method
    relation.show.return_value = None
    
    return relation


@pytest.fixture
def mock_s3_storage():
    """Fixture to mock S3 storage operations."""
    with patch('duckdb.DuckDBPyConnection.execute') as mock_execute:
        # Configure the mock to return a success response for COPY operations
        mock_execute.return_value = None
        yield mock_execute
