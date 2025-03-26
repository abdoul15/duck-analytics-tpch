import pytest
from unittest.mock import patch, MagicMock, call
import duckdb
from analytics.utils.duck_etl_base import TableETL, InValidDataException
from analytics.utils.etl_dataset import ETLDataSet


# Create a concrete implementation of the abstract TableETL class for testing
class TestableTableETL(TableETL):
    def __init__(
        self,
        conn,
        upstream_table_names=None,
        name="test_table",
        primary_keys=["id"],
        storage_path="s3://test-bucket/test-path",
        data_format="parquet",
        database="test_db",
        partition_keys=["etl_inserted"],
        run_upstream=True,
        load_data=True,
    ):
        super().__init__(
            conn,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )
    
    def extract_upstream(self):
        # Mock implementation for testing
        relation = self.conn.table("test_customer")
        return [
            ETLDataSet(
                name=self.name,
                curr_data=relation,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )
        ]
    
    def transform_upstream(self, upstream_datasets):
        # Mock implementation for testing
        return upstream_datasets[0]
    
    def read(self, partition_values=None):
        # Mock implementation for testing
        relation = self.conn.table("test_customer")
        return ETLDataSet(
            name=self.name,
            curr_data=relation,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )


class TestTableETL:
    
    def test_initialization(self, mock_duckdb_connection):
        """Test that TableETL is correctly initialized with all required attributes."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        assert etl.conn == mock_duckdb_connection
        assert etl.name == "test_table"
        assert etl.primary_keys == ["id"]
        assert etl.storage_path == "s3://test-bucket/test-path"
        assert etl.data_format == "parquet"
        assert etl.database == "test_db"
        assert etl.partition_keys == ["etl_inserted"]
        assert etl.run_upstream is True
        assert etl.load_data is True
        assert etl.curr_data is None
    
    def test_run_success(self, mock_duckdb_connection):
        """Test that run() correctly executes the ETL process when validation passes."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        # Mock the validate method to return True
        with patch.object(etl, 'validate', return_value=True):
            # Mock the load method
            with patch.object(etl, 'load') as mock_load:
                etl.run()
                
                # Check that load was called with the correct dataset
                mock_load.assert_called_once()
                dataset_arg = mock_load.call_args[0][0]
                assert isinstance(dataset_arg, ETLDataSet)
                assert dataset_arg.name == "test_table"
    
    def test_run_validation_failure(self, mock_duckdb_connection):
        """Test that run() raises InValidDataException when validation fails."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        # Mock the validate method to return False
        with patch.object(etl, 'validate', return_value=False):
            # Mock the load method
            with patch.object(etl, 'load') as mock_load:
                with pytest.raises(InValidDataException) as excinfo:
                    etl.run()
                
                # Check that the error message mentions validation
                assert "did not pass validation" in str(excinfo.value)
                
                # Check that load was not called
                mock_load.assert_not_called()
    
    def test_load(self, mock_duckdb_connection, mock_relation):
        """Test that load() correctly executes the COPY command."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        # Create a dataset to load
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=["id"],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=["etl_inserted"]
        )
        
        # Call the load method
        etl.load(dataset)
        
        # Check that the correct SQL commands were executed
        # First, a view should be created
        mock_relation.create_view.assert_called_once_with("tmp_to_write", replace=True)
        
        # Then, a COPY command should be executed
        # We can't easily check the exact SQL due to formatting differences,
        # but we can check that it contains the key parts
        copy_calls = [call for call in mock_duckdb_connection.execute.call_args_list 
                     if isinstance(call[0][0], str) and "COPY tmp_to_write" in call[0][0]]
        assert len(copy_calls) == 1
        copy_sql = copy_calls[0][0][0]
        assert "TO 's3://test-bucket/test-path'" in copy_sql
        assert "FORMAT 'parquet'" in copy_sql
        assert "PARTITION_BY (etl_inserted)" in copy_sql
    
    def test_validate_no_expectations_file(self, mock_duckdb_connection, mock_relation):
        """Test that validate() returns True when no expectations file exists."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        # Create a dataset to validate
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=["id"],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=["etl_inserted"]
        )
        
        # Mock Path.exists to return False (no expectations file)
        with patch('pathlib.Path.exists', return_value=False):
            result = etl.validate(dataset)
            assert result is True
    
    @patch('great_expectations.get_context')
    def test_validate_with_expectations_success(self, mock_get_context, mock_duckdb_connection, mock_relation):
        """Test that validate() correctly uses Great Expectations when an expectations file exists."""
        etl = TestableTableETL(mock_duckdb_connection)
        
        # Create a dataset to validate
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=["id"],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=["etl_inserted"]
        )
        
        # Mock Path.exists to return True (expectations file exists)
        with patch('pathlib.Path.exists', return_value=True):
            # Mock the Great Expectations context and related objects
            mock_context = MagicMock()
            mock_get_context.return_value = mock_context
            
            mock_datasource = MagicMock()
            mock_context.sources.add_or_update_pandas.return_value = mock_datasource
            
            mock_asset = MagicMock()
            mock_datasource.add_dataframe_asset.return_value = mock_asset
            
            mock_batch_request = MagicMock()
            mock_asset.build_batch_request.return_value = mock_batch_request
            
            mock_checkpoint = MagicMock()
            mock_context.add_or_update_checkpoint.return_value = mock_checkpoint
            
            # Mock the checkpoint run to return success
            mock_result = MagicMock()
            mock_result.success = True
            mock_checkpoint.run.return_value = mock_result
            
            # Mock the DuckDB relation to Pandas conversion
            mock_relation.df.return_value = "mock_pandas_df"
            
            # Call validate
            result = etl.validate(dataset)
            
            # Check that the result is True
            assert result is True
            
            # Check that the Great Expectations methods were called correctly
            mock_get_context.assert_called_once()
            mock_context.sources.add_or_update_pandas.assert_called_once_with(name='temp_duckdb_datasource')
            mock_datasource.add_dataframe_asset.assert_called_once_with(name='test_table')
            mock_asset.build_batch_request.assert_called_once_with(dataframe='mock_pandas_df')
            mock_context.add_or_update_checkpoint.assert_called_once()
            mock_checkpoint.run.assert_called_once()
