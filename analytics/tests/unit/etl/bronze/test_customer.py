import pytest
from unittest.mock import patch, MagicMock
import duckdb
from datetime import datetime
from analytics.etl.bronze.customer import CustomerBronzeETL
from analytics.utils.etl_dataset import ETLDataSet


class TestCustomerBronzeETL:
    
    def test_initialization(self, mock_duckdb_connection):
        """Test that CustomerBronzeETL is correctly initialized with default parameters."""
        etl = CustomerBronzeETL(conn=mock_duckdb_connection)
        
        assert etl.conn == mock_duckdb_connection
        assert etl.name == 'customer'
        assert etl.primary_keys == ['c_custkey']
        assert etl.storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert etl.data_format == 'parquet'
        assert etl.database == 'tpchdb'
        assert etl.partition_keys == ['etl_inserted']
        assert etl.run_upstream is True
        assert etl.load_data is True
    
    def test_initialization_with_custom_parameters(self, mock_duckdb_connection):
        """Test that CustomerBronzeETL is correctly initialized with custom parameters."""
        etl = CustomerBronzeETL(
            conn=mock_duckdb_connection,
            upstream_table_names=['some_upstream'],
            name='custom_customer',
            primary_keys=['custom_key'],
            storage_path='custom/path',
            data_format='csv',
            database='custom_db',
            partition_keys=['custom_partition'],
            run_upstream=False,
            load_data=False
        )
        
        assert etl.conn == mock_duckdb_connection
        assert etl.upstream_table_names == ['some_upstream']
        assert etl.name == 'custom_customer'
        assert etl.primary_keys == ['custom_key']
        assert etl.storage_path == 'custom/path'
        assert etl.data_format == 'csv'
        assert etl.database == 'custom_db'
        assert etl.partition_keys == ['custom_partition']
        assert etl.run_upstream is False
        assert etl.load_data is False
    
    def test_extract_upstream(self, mock_duckdb_connection, mock_relation):
        """Test that extract_upstream correctly extracts data from the upstream source."""
        # Create the ETL instance
        etl = CustomerBronzeETL(conn=mock_duckdb_connection)
        
        # Mock the get_table_from_db function directly on the instance
        with patch('analytics.etl.bronze.customer.get_table_from_db', return_value=mock_relation) as mock_get_table:
            # Call extract_upstream
            result = etl.extract_upstream()
            
            # Check that get_table_from_db was called with the correct parameters
            mock_get_table.assert_called_once_with(
                conn=mock_duckdb_connection,
                table_name='public.customer'
            )
        
        # Check that the result is a list containing one ETLDataSet
        assert len(result) == 1
        assert isinstance(result[0], ETLDataSet)
        assert result[0].name == 'customer'
        assert result[0].curr_data == mock_relation
        assert result[0].primary_keys == ['c_custkey']
        assert result[0].storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert result[0].data_format == 'parquet'
        assert result[0].database == 'tpchdb'
        assert result[0].partition_keys == ['etl_inserted']
    
    def test_transform_upstream(self, mock_duckdb_connection, mock_relation):
        """Test that transform_upstream correctly transforms the upstream data."""
        # Create the ETL instance
        etl = CustomerBronzeETL(conn=mock_duckdb_connection)
        
        # Create a mock upstream dataset
        upstream_dataset = ETLDataSet(
            name='customer',
            curr_data=mock_relation,
            primary_keys=['c_custkey'],
            storage_path='s3://duckdb-bucket-tpch/bronze/customer',
            data_format='parquet',
            database='tpchdb',
            partition_keys=['etl_inserted']
        )
        
        # Mock the from_query method to return a transformed relation
        transformed_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.from_query.return_value = transformed_relation
        
        # Directly patch the datetime in the CustomerBronzeETL module
        with patch('analytics.etl.bronze.customer.datetime') as mock_datetime:
            # Mock the datetime.now() to return a fixed timestamp
            mock_now = MagicMock()
            mock_now.isoformat.return_value = '2023-01-01T12:00:00'
            mock_datetime.now.return_value = mock_now
            
            # Mock the SQL query directly
            mock_duckdb_connection.from_query.side_effect = lambda query: (
                transformed_relation if "TIMESTAMP '2023-01-01T12:00:00' AS etl_inserted" in query else None
            )
            
            result = etl.transform_upstream([upstream_dataset])
        
        # Check that create_view was called on the input relation
        mock_relation.create_view.assert_called_once_with("tmp_customer_input", replace=True)
        
        # Check that from_query was called
        mock_duckdb_connection.from_query.assert_called_once()
        
        # Check that the result is an ETLDataSet with the transformed relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'customer'
        assert result.curr_data == transformed_relation
        assert result.primary_keys == ['c_custkey']
        assert result.storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
    
    def test_read_with_in_memory_data(self, mock_duckdb_connection, mock_relation):
        """Test that read() returns the in-memory data when load_data=False and curr_data is not None."""
        # Create the ETL instance with load_data=False
        etl = CustomerBronzeETL(conn=mock_duckdb_connection, load_data=False)
        
        # Set curr_data to a mock relation
        etl.curr_data = mock_relation
        
        # Call read
        result = etl.read()
        
        # Check that the result is an ETLDataSet with the in-memory relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'customer'
        assert result.curr_data == mock_relation
        assert result.primary_keys == ['c_custkey']
        assert result.storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
    
    def test_read_all_partitions(self, mock_duckdb_connection):
        """Test that read() correctly reads all partitions when partition_values is None."""
        # Create the ETL instance
        etl = CustomerBronzeETL(conn=mock_duckdb_connection)
        
        # Mock the read_parquet method to return a relation
        all_partitions_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.read_parquet.return_value = all_partitions_relation
        
        # Mock the execute method to return a result with a latest timestamp
        mock_result = MagicMock()
        mock_result.fetchone.return_value = ['2023-01-01T12:00:00']
        mock_duckdb_connection.execute.return_value = mock_result
        
        # Mock the from_query method to return a filtered relation
        filtered_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.from_query.return_value = filtered_relation
        
        # Call read with no partition_values
        result = etl.read()
        
        # Check that read_parquet was called with the correct path
        mock_duckdb_connection.read_parquet.assert_called_once_with(
            "s3://duckdb-bucket-tpch/bronze/customer/*/*.parquet",
            hive_partitioning=True
        )
        
        # Check that create_view was called on the all_partitions relation
        all_partitions_relation.create_view.assert_called_once_with("all_partitions", replace=True)
        
        # Check that execute was called to get the latest timestamp
        mock_duckdb_connection.execute.assert_called_once_with("SELECT max(etl_inserted) FROM all_partitions")
        
        # Check that from_query was called with the correct SQL to filter by the latest timestamp
        mock_duckdb_connection.from_query.assert_called_once_with(
            "SELECT * FROM all_partitions WHERE etl_inserted = '2023-01-01T12:00:00'"
        )
        
        # Check that the result is an ETLDataSet with the filtered relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'customer'
        assert result.curr_data == filtered_relation
        assert result.primary_keys == ['c_custkey']
        assert result.storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
    
    def test_read_specific_partition(self, mock_duckdb_connection):
        """Test that read() correctly reads a specific partition when partition_values is provided."""
        # Create the ETL instance
        etl = CustomerBronzeETL(conn=mock_duckdb_connection)
        
        # Mock the read_parquet method to return a relation
        partition_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.read_parquet.return_value = partition_relation
        
        # Call read with specific partition_values
        result = etl.read(partition_values={'etl_inserted': '2023-01-01T12:00:00'})
        
        # Check that read_parquet was called with the correct path
        mock_duckdb_connection.read_parquet.assert_called_once_with(
            "s3://duckdb-bucket-tpch/bronze/customer/etl_inserted=2023-01-01T12:00:00/*.parquet",
            hive_partitioning=True
        )
        
        # Check that the result is an ETLDataSet with the partition relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'customer'
        assert result.curr_data == partition_relation
        assert result.primary_keys == ['c_custkey']
        assert result.storage_path == 's3://duckdb-bucket-tpch/bronze/customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
