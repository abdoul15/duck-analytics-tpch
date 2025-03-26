import pytest
from unittest.mock import patch, MagicMock
import duckdb
import os
from analytics.utils.duck_database import get_table_from_db


class TestDuckDatabase:
    
    def test_get_table_from_db_missing_env_vars(self):
        """Test that get_table_from_db raises an error when environment variables are missing."""
        # Mock empty environment
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError) as excinfo:
                conn = MagicMock(spec=duckdb.DuckDBPyConnection)
                get_table_from_db(conn, "test_table")
            
            # Check that the error message mentions missing variables
            assert "Variables manquantes" in str(excinfo.value)
    
    def test_get_table_from_db_with_schema(self, mock_env_vars):
        """Test that get_table_from_db correctly handles table names with schema."""
        conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        conn.from_query.return_value = "mock_relation"
        
        # Call with schema.table format
        result = get_table_from_db(conn, "custom_schema.test_table")
        
        # Check that the correct query was executed
        expected_query_part = "postgres_scan('postgresql://test_user:test_password@localhost:5432/test_db', 'custom_schema', 'test_table')"
        conn.from_query.assert_called_once()
        actual_query = conn.from_query.call_args[0][0]
        assert expected_query_part in actual_query
        assert result == "mock_relation"
    
    def test_get_table_from_db_without_schema(self, mock_env_vars):
        """Test that get_table_from_db correctly handles table names without schema."""
        conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        conn.from_query.return_value = "mock_relation"
        
        # Call with just table name (no schema)
        result = get_table_from_db(conn, "test_table")
        
        # Check that the correct query was executed with default 'public' schema
        expected_query_part = "postgres_scan('postgresql://test_user:test_password@localhost:5432/test_db', 'public', 'test_table')"
        conn.from_query.assert_called_once()
        actual_query = conn.from_query.call_args[0][0]
        assert expected_query_part in actual_query
        assert result == "mock_relation"
    
    def test_get_table_from_db_installs_postgres_scanner(self, mock_env_vars):
        """Test that get_table_from_db installs and loads the postgres_scanner plugin."""
        conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        conn.from_query.return_value = "mock_relation"
        
        get_table_from_db(conn, "test_table")
        
        # Check that the postgres_scanner plugin was installed and loaded
        assert conn.execute.call_count == 2
        conn.execute.assert_any_call("INSTALL postgres_scanner")
        conn.execute.assert_any_call("LOAD postgres_scanner")
