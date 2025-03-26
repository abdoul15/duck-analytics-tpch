import pytest
from unittest.mock import patch, MagicMock, call
import duckdb
from datetime import datetime
from analytics.etl.silver.dim_customer import DimCustomerSilverETL
from analytics.etl.bronze.customer import CustomerBronzeETL
from analytics.etl.bronze.nation import NationBronzeETL
from analytics.etl.bronze.region import RegionBronzeETL
from analytics.utils.etl_dataset import ETLDataSet


class TestDimCustomerSilverETL:
    
    def test_initialization(self, mock_duckdb_connection):
        """Test that DimCustomerSilverETL is correctly initialized with default parameters."""
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection)
        
        assert etl.conn == mock_duckdb_connection
        assert etl.upstream_table_names == [CustomerBronzeETL, NationBronzeETL, RegionBronzeETL]
        assert etl.name == 'dim_customer'
        assert etl.primary_keys == ['customer_key']
        assert etl.storage_path == 's3://duckdb-bucket-tpch/silver/dim_customer'
        assert etl.data_format == 'parquet'
        assert etl.database == 'tpchdb'
        assert etl.partition_keys == ['etl_inserted']
        assert etl.run_upstream is True
        assert etl.load_data is True
    
    def test_initialization_with_custom_parameters(self, mock_duckdb_connection):
        """Test that DimCustomerSilverETL is correctly initialized with custom parameters."""
        etl = DimCustomerSilverETL(
            conn=mock_duckdb_connection,
            upstream_table_names=['custom_upstream'],
            name='custom_dim_customer',
            primary_keys=['custom_key'],
            storage_path='custom/path',
            data_format='csv',
            database='custom_db',
            partition_keys=['custom_partition'],
            run_upstream=False,
            load_data=False
        )
        
        assert etl.conn == mock_duckdb_connection
        assert etl.upstream_table_names == ['custom_upstream']
        assert etl.name == 'custom_dim_customer'
        assert etl.primary_keys == ['custom_key']
        assert etl.storage_path == 'custom/path'
        assert etl.data_format == 'csv'
        assert etl.database == 'custom_db'
        assert etl.partition_keys == ['custom_partition']
        assert etl.run_upstream is False
        assert etl.load_data is False
    
    @patch.object(CustomerBronzeETL, 'run')
    @patch.object(CustomerBronzeETL, 'read')
    @patch.object(NationBronzeETL, 'run')
    @patch.object(NationBronzeETL, 'read')
    @patch.object(RegionBronzeETL, 'run')
    @patch.object(RegionBronzeETL, 'read')
    def test_extract_upstream_with_run_upstream(
        self, mock_region_read, mock_region_run, mock_nation_read, mock_nation_run, 
        mock_customer_read, mock_customer_run, mock_duckdb_connection, mock_relation
    ):
        """Test that extract_upstream correctly extracts data from upstream sources with run_upstream=True."""
        # Configure the mocks to return mock datasets
        customer_dataset = ETLDataSet(
            name='customer',
            curr_data=mock_relation,
            primary_keys=['c_custkey'],
            storage_path='s3://test/customer',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_customer_read.return_value = customer_dataset
        
        nation_dataset = ETLDataSet(
            name='nation',
            curr_data=mock_relation,
            primary_keys=['n_nationkey'],
            storage_path='s3://test/nation',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_nation_read.return_value = nation_dataset
        
        region_dataset = ETLDataSet(
            name='region',
            curr_data=mock_relation,
            primary_keys=['r_regionkey'],
            storage_path='s3://test/region',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_region_read.return_value = region_dataset
        
        # Create the ETL instance with run_upstream=True
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection, run_upstream=True)
        
        # Call extract_upstream
        result = etl.extract_upstream()
        
        # Check that run was called on each upstream ETL
        mock_customer_run.assert_called_once()
        mock_nation_run.assert_called_once()
        mock_region_run.assert_called_once()
        
        # Check that read was called on each upstream ETL
        mock_customer_read.assert_called_once()
        mock_nation_read.assert_called_once()
        mock_region_read.assert_called_once()
        
        # Check that the result is a list containing the three datasets
        assert len(result) == 3
        assert result[0] == customer_dataset
        assert result[1] == nation_dataset
        assert result[2] == region_dataset
    
    @patch.object(CustomerBronzeETL, 'run')
    @patch.object(CustomerBronzeETL, 'read')
    @patch.object(NationBronzeETL, 'run')
    @patch.object(NationBronzeETL, 'read')
    @patch.object(RegionBronzeETL, 'run')
    @patch.object(RegionBronzeETL, 'read')
    def test_extract_upstream_without_run_upstream(
        self, mock_region_read, mock_region_run, mock_nation_read, mock_nation_run, 
        mock_customer_read, mock_customer_run, mock_duckdb_connection, mock_relation
    ):
        """Test that extract_upstream correctly extracts data from upstream sources with run_upstream=False."""
        # Configure the mocks to return mock datasets
        customer_dataset = ETLDataSet(
            name='customer',
            curr_data=mock_relation,
            primary_keys=['c_custkey'],
            storage_path='s3://test/customer',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_customer_read.return_value = customer_dataset
        
        nation_dataset = ETLDataSet(
            name='nation',
            curr_data=mock_relation,
            primary_keys=['n_nationkey'],
            storage_path='s3://test/nation',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_nation_read.return_value = nation_dataset
        
        region_dataset = ETLDataSet(
            name='region',
            curr_data=mock_relation,
            primary_keys=['r_regionkey'],
            storage_path='s3://test/region',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        mock_region_read.return_value = region_dataset
        
        # Create the ETL instance with run_upstream=False
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection, run_upstream=False)
        
        # Call extract_upstream
        result = etl.extract_upstream()
        
        # Check that run was not called on any upstream ETL
        mock_customer_run.assert_not_called()
        mock_nation_run.assert_not_called()
        mock_region_run.assert_not_called()
        
        # Check that read was called on each upstream ETL
        mock_customer_read.assert_called_once()
        mock_nation_read.assert_called_once()
        mock_region_read.assert_called_once()
        
        # Check that the result is a list containing the three datasets
        assert len(result) == 3
        assert result[0] == customer_dataset
        assert result[1] == nation_dataset
        assert result[2] == region_dataset
    
    def test_transform_upstream(self, mock_duckdb_connection, mock_relation):
        """Test that transform_upstream correctly transforms the upstream data."""
        # Create the ETL instance
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection)
        
        # Create mock upstream datasets
        customer_dataset = ETLDataSet(
            name='customer',
            curr_data=mock_relation,
            primary_keys=['c_custkey'],
            storage_path='s3://test/customer',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        
        nation_dataset = ETLDataSet(
            name='nation',
            curr_data=mock_relation,
            primary_keys=['n_nationkey'],
            storage_path='s3://test/nation',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        
        region_dataset = ETLDataSet(
            name='region',
            curr_data=mock_relation,
            primary_keys=['r_regionkey'],
            storage_path='s3://test/region',
            data_format='parquet',
            database='test_db',
            partition_keys=['etl_inserted']
        )
        
        # Mock the from_query method to return a transformed relation
        transformed_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.from_query.return_value = transformed_relation
        
        # Directly patch the datetime in the DimCustomerSilverETL module
        with patch('analytics.etl.silver.dim_customer.datetime') as mock_datetime:
            # Mock the datetime.now() to return a fixed timestamp
            mock_now = MagicMock()
            mock_now.isoformat.return_value = '2023-01-01T12:00:00'
            mock_datetime.now.return_value = mock_now
            
            # Mock the SQL query directly
            mock_duckdb_connection.from_query.side_effect = lambda query: (
                transformed_relation if "'2023-01-01T12:00:00' AS etl_inserted" in query else None
            )
            
            result = etl.transform_upstream([customer_dataset, nation_dataset, region_dataset])
        
        # Check that create_view was called on each input relation
        mock_relation.create_view.assert_any_call("customer_temp", replace=True)
        mock_relation.create_view.assert_any_call("nation_temp", replace=True)
        mock_relation.create_view.assert_any_call("region_temp", replace=True)
        assert mock_relation.create_view.call_count == 3
        
        # Check that from_query was called
        mock_duckdb_connection.from_query.assert_called_once()
        
        # Check that the result is an ETLDataSet with the transformed relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'dim_customer'
        assert result.curr_data == transformed_relation
        assert result.primary_keys == ['customer_key']
        assert result.storage_path == 's3://duckdb-bucket-tpch/silver/dim_customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
        
        # Check that curr_data was set on the ETL instance
        assert etl.curr_data == transformed_relation
    
    def test_read_with_in_memory_data(self, mock_duckdb_connection, mock_relation):
        """Test that read() returns the in-memory data when load_data=False and curr_data is not None."""
        # Create the ETL instance with load_data=False
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection, load_data=False)
        
        # Set curr_data to a mock relation
        etl.curr_data = mock_relation
        
        # Call read
        result = etl.read()
        
        # Check that the result is an ETLDataSet with the in-memory relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'dim_customer'
        assert result.curr_data == mock_relation
        assert result.primary_keys == ['customer_key']
        assert result.storage_path == 's3://duckdb-bucket-tpch/silver/dim_customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
    
    def test_read_all_partitions(self, mock_duckdb_connection):
        """Test that read() correctly reads all partitions when partition_values is None."""
        # Create the ETL instance
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection)
        
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
            "s3://duckdb-bucket-tpch/silver/dim_customer/*/*.parquet",
            hive_partitioning=True
        )
        
        # Check that create_view was called on the all_partitions relation
        all_partitions_relation.create_view.assert_called_once_with("dim_customer_all", replace=True)
        
        # Check that execute was called to get the latest timestamp
        mock_duckdb_connection.execute.assert_called_once_with("SELECT max(etl_inserted) FROM dim_customer_all")
        
        # Check that from_query was called with the correct SQL to filter by the latest timestamp
        mock_duckdb_connection.from_query.assert_called_once_with(
            "SELECT * FROM dim_customer_all WHERE etl_inserted = '2023-01-01T12:00:00'"
        )
        
        # Check that the result is an ETLDataSet with the filtered relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'dim_customer'
        assert result.curr_data == filtered_relation
        assert result.primary_keys == ['customer_key']
        assert result.storage_path == 's3://duckdb-bucket-tpch/silver/dim_customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
    
    def test_read_specific_partition(self, mock_duckdb_connection):
        """Test that read() correctly reads a specific partition when partition_values is provided."""
        # Create the ETL instance
        etl = DimCustomerSilverETL(conn=mock_duckdb_connection)
        
        # Mock the read_parquet method to return a relation
        partition_relation = MagicMock(spec=duckdb.DuckDBPyRelation)
        mock_duckdb_connection.read_parquet.return_value = partition_relation
        
        # Call read with specific partition_values
        result = etl.read(partition_values={'etl_inserted': '2023-01-01T12:00:00'})
        
        # Check that read_parquet was called with the correct path
        mock_duckdb_connection.read_parquet.assert_called_once_with(
            "s3://duckdb-bucket-tpch/silver/dim_customer/etl_inserted=2023-01-01T12:00:00/*.parquet",
            hive_partitioning=True
        )
        
        # Check that the result is an ETLDataSet with the partition relation
        assert isinstance(result, ETLDataSet)
        assert result.name == 'dim_customer'
        assert result.curr_data == partition_relation
        assert result.primary_keys == ['customer_key']
        assert result.storage_path == 's3://duckdb-bucket-tpch/silver/dim_customer'
        assert result.data_format == 'parquet'
        assert result.database == 'tpchdb'
        assert result.partition_keys == ['etl_inserted']
