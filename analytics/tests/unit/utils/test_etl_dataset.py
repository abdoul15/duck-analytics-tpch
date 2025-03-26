import pytest
from analytics.utils.etl_dataset import ETLDataSet


class TestETLDataSet:
    
    def test_etl_dataset_initialization(self, mock_relation):
        """Test that ETLDataSet is correctly initialized with all required attributes."""
        # Create an ETLDataSet instance
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=["c_custkey"],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=["etl_inserted"]
        )
        
        # Check that all attributes are correctly set
        assert dataset.name == "test_dataset"
        assert dataset.curr_data == mock_relation
        assert dataset.primary_keys == ["c_custkey"]
        assert dataset.storage_path == "s3://test-bucket/test-path"
        assert dataset.data_format == "parquet"
        assert dataset.database == "test_db"
        assert dataset.partition_keys == ["etl_inserted"]
    
    def test_etl_dataset_with_empty_primary_keys(self, mock_relation):
        """Test that ETLDataSet can be initialized with empty primary_keys."""
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=[],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=[]
        )
        
        assert dataset.primary_keys == []
        assert dataset.partition_keys == []
    
    def test_etl_dataset_with_multiple_primary_keys(self, mock_relation):
        """Test that ETLDataSet can be initialized with multiple primary_keys."""
        dataset = ETLDataSet(
            name="test_dataset",
            curr_data=mock_relation,
            primary_keys=["key1", "key2", "key3"],
            storage_path="s3://test-bucket/test-path",
            data_format="parquet",
            database="test_db",
            partition_keys=["part1", "part2"]
        )
        
        assert dataset.primary_keys == ["key1", "key2", "key3"]
        assert dataset.partition_keys == ["part1", "part2"]
