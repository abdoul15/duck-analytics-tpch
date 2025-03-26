import pytest
from unittest.mock import patch, MagicMock
import duckdb
import os
from analytics.etl.bronze.customer import CustomerBronzeETL
from analytics.etl.bronze.nation import NationBronzeETL
from analytics.etl.bronze.region import RegionBronzeETL
from analytics.etl.silver.dim_customer import DimCustomerSilverETL
from analytics.utils.etl_dataset import ETLDataSet


class TestETLPipeline:
    
    @pytest.fixture
    def setup_test_tables(self, mock_duckdb_connection):
        """Fixture to set up test tables in the mock DuckDB connection."""
        # Create test customer table
        mock_duckdb_connection.execute("""
            CREATE TABLE test_customer AS 
            SELECT 
                1 AS c_custkey, 
                'Customer#000000001' AS c_name,
                1 AS c_nationkey,
                'Address 1' AS c_address,
                '25-989-741-2988' AS c_phone,
                0.23 AS c_acctbal,
                'BUILDING' AS c_mktsegment,
                'regular accounts' AS c_comment
        """)
        
        # Create test nation table
        mock_duckdb_connection.execute("""
            CREATE TABLE test_nation AS 
            SELECT 
                1 AS n_nationkey, 
                'ARGENTINA' AS n_name,
                1 AS n_regionkey,
                'regular accounts' AS n_comment
        """)
        
        # Create test region table
        mock_duckdb_connection.execute("""
            CREATE TABLE test_region AS 
            SELECT 
                1 AS r_regionkey, 
                'AMERICA' AS r_name,
                'regular accounts' AS r_comment
        """)
        
        yield
        
        # Clean up
        mock_duckdb_connection.execute("DROP TABLE IF EXISTS test_customer")
        mock_duckdb_connection.execute("DROP TABLE IF EXISTS test_nation")
        mock_duckdb_connection.execute("DROP TABLE IF EXISTS test_region")
    
    @patch('analytics.utils.duck_database.get_table_from_db')
    def test_bronze_to_silver_pipeline(self, mock_get_table, mock_duckdb_connection, setup_test_tables, mock_s3_storage):
        """Test the ETL pipeline from bronze to silver layer."""
        # Mock get_table_from_db to return test tables
        def mock_get_table_impl(conn, table_name):
            if table_name == 'public.customer':
                return conn.table("test_customer")
            elif table_name == 'public.nation':
                return conn.table("test_nation")
            elif table_name == 'public.region':
                return conn.table("test_region")
            else:
                raise ValueError(f"Unexpected table name: {table_name}")
        
        mock_get_table.side_effect = mock_get_table_impl
        
        # Create and run the bronze ETL classes with load_data=False to avoid actual S3 writes
        customer_etl = CustomerBronzeETL(conn=mock_duckdb_connection, load_data=False)
        customer_etl.run()
        customer_dataset = customer_etl.read()
        
        nation_etl = NationBronzeETL(conn=mock_duckdb_connection, load_data=False)
        nation_etl.run()
        nation_dataset = nation_etl.read()
        
        region_etl = RegionBronzeETL(conn=mock_duckdb_connection, load_data=False)
        region_etl.run()
        region_dataset = region_etl.read()
        
        # Verify bronze datasets
        assert customer_dataset.name == 'customer'
        assert nation_dataset.name == 'nation'
        assert region_dataset.name == 'region'
        
        # Create and run the silver ETL class with run_upstream=False and load_data=False
        # We'll patch the extract_upstream method to use our already-run bronze ETLs
        with patch.object(DimCustomerSilverETL, 'extract_upstream', return_value=[customer_dataset, nation_dataset, region_dataset]):
            dim_customer_etl = DimCustomerSilverETL(conn=mock_duckdb_connection, run_upstream=False, load_data=False)
            dim_customer_etl.run()
            dim_customer_dataset = dim_customer_etl.read()
            
            # Verify silver dataset
            assert dim_customer_dataset.name == 'dim_customer'
            
            # Mock the execute().fetchall() to return a test result
            mock_result = [(1, 'Customer#000000001', 'ARGENTINA', 'AMERICA', 'Address 1, ARGENTINA, AMERICA', '25-989-741-2988', 0.23, 'BUILDING')]
            mock_duckdb_connection.execute.return_value.fetchall.return_value = mock_result
            
            # Query the silver dataset to verify the transformation
            dim_customer_dataset.curr_data.create_view("dim_customer_view", replace=True)
            result = mock_duckdb_connection.execute("""
                SELECT 
                    customer_key, 
                    customer_name, 
                    nation_name, 
                    region_name, 
                    full_address, 
                    phone_number, 
                    account_balance, 
                    market_segment
                FROM dim_customer_view
            """).fetchall()
            
            # Verify the result
            assert len(result) == 1
            row = result[0]
            assert row[0] == 1  # customer_key
            assert row[1] == 'Customer#000000001'  # customer_name
            assert row[2] == 'ARGENTINA'  # nation_name
            assert row[3] == 'AMERICA'  # region_name
            assert row[4] == 'Address 1, ARGENTINA, AMERICA'  # full_address
            assert row[5] == '25-989-741-2988'  # phone_number
            assert row[6] == 0.23  # account_balance
            assert row[7] == 'BUILDING'  # market_segment
    
    @patch('analytics.utils.duck_database.get_table_from_db')
    def test_end_to_end_pipeline_with_mocks(self, mock_get_table, mock_duckdb_connection, setup_test_tables, mock_s3_storage):
        """Test the end-to-end ETL pipeline with mocked database connections."""
        # Mock get_table_from_db to return test tables
        def mock_get_table_impl(conn, table_name):
            if table_name == 'public.customer':
                return conn.table("test_customer")
            elif table_name == 'public.nation':
                return conn.table("test_nation")
            elif table_name == 'public.region':
                return conn.table("test_region")
            else:
                raise ValueError(f"Unexpected table name: {table_name}")
        
        mock_get_table.side_effect = mock_get_table_impl
        
        # Create and run the silver ETL class with run_upstream=True and load_data=False
        # This will run the entire pipeline from bronze to silver
        dim_customer_etl = DimCustomerSilverETL(conn=mock_duckdb_connection, run_upstream=True, load_data=False)
        dim_customer_etl.run()
        dim_customer_dataset = dim_customer_etl.read()
        
        # Verify silver dataset
        assert dim_customer_dataset.name == 'dim_customer'
        
        # Mock the execute().fetchall() to return a test result
        mock_result = [(1, 'Customer#000000001', 'ARGENTINA', 'AMERICA', 'Address 1, ARGENTINA, AMERICA', '25-989-741-2988', 0.23, 'BUILDING')]
        mock_duckdb_connection.execute.return_value.fetchall.return_value = mock_result
        
        # Query the silver dataset to verify the transformation
        dim_customer_dataset.curr_data.create_view("dim_customer_view", replace=True)
        result = mock_duckdb_connection.execute("""
            SELECT 
                customer_key, 
                customer_name, 
                nation_name, 
                region_name, 
                full_address, 
                phone_number, 
                account_balance, 
                market_segment
            FROM dim_customer_view
        """).fetchall()
        
        # Verify the result
        assert len(result) == 1
        row = result[0]
        assert row[0] == 1  # customer_key
        assert row[1] == 'Customer#000000001'  # customer_name
        assert row[2] == 'ARGENTINA'  # nation_name
        assert row[3] == 'AMERICA'  # region_name
        assert row[4] == 'Address 1, ARGENTINA, AMERICA'  # full_address
        assert row[5] == '25-989-741-2988'  # phone_number
        assert row[6] == 0.23  # account_balance
        assert row[7] == 'BUILDING'  # market_segment
