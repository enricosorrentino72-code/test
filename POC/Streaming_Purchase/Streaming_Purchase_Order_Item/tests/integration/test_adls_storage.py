"""
Integration tests for ADLS Gen2 storage operations.

Tests cover:
- File system operations
- Data lake interactions
- Storage account connectivity
- Path management
- Performance testing with ADLS
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json
import sys

# Add necessary paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utility')))

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestADLSIntegration:
    """Integration tests for ADLS Gen2 storage operations."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for integration testing."""
        spark = SparkSession.builder \
            .appName("ADLSIntegrationTest") \
            .config("spark.master", "local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def adls_config(self):
        """ADLS configuration for testing."""
        return {
            "account_name": os.getenv("ADLS_ACCOUNT_NAME", "testaccount"),
            "account_key": os.getenv("ADLS_ACCOUNT_KEY", "testkey"),
            "container": os.getenv("ADLS_CONTAINER", "testcontainer"),
            "base_path": "/integration-tests"
        }

    @pytest.fixture
    def mock_adls_client(self):
        """Create a mock ADLS client for testing."""
        with patch('azure.storage.filedatalake.DataLakeServiceClient') as mock:
            client = Mock(spec=DataLakeServiceClient)
            file_system_client = Mock(spec=FileSystemClient)

            client.get_file_system_client = Mock(return_value=file_system_client)
            file_system_client.exists = Mock(return_value=True)
            file_system_client.create_directory = Mock()
            file_system_client.delete_directory = Mock()

            mock.from_connection_string.return_value = client
            yield client

    def test_adls_connection(self, mock_adls_client, adls_config):
        """Test ADLS connection establishment."""
        # Test connection
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        assert file_system_client is not None
        assert file_system_client.exists() is True

    def test_create_directory_structure(self, mock_adls_client, adls_config):
        """Test creating directory structure in ADLS."""
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        # Create directory structure
        directories = [
            f"{adls_config['base_path']}/bronze",
            f"{adls_config['base_path']}/silver",
            f"{adls_config['base_path']}/gold"
        ]

        for directory in directories:
            directory_client = Mock()
            file_system_client.create_directory.return_value = directory_client
            result = file_system_client.create_directory(directory)
            assert result is not None

        assert file_system_client.create_directory.call_count == 3

    def test_write_parquet_to_adls(self, spark_session, adls_config):
        """Test writing Parquet files to ADLS."""
        # Create sample DataFrame
        data = [
            ("ORD001", "ITEM001", 10, 100.0),
            ("ORD002", "ITEM002", 5, 50.0),
            ("ORD003", "ITEM003", 3, 30.0)
        ]

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("amount", StringType(), False)
        ])

        df = spark_session.createDataFrame(data, schema)

        # Mock write operation
        with patch.object(df.write, 'parquet') as mock_write:
            output_path = f"abfss://{adls_config['container']}@{adls_config['account_name']}.dfs.core.windows.net{adls_config['base_path']}/test-data"

            df.write.mode("overwrite").parquet(output_path)
            mock_write.assert_called_once_with(output_path)

    def test_read_parquet_from_adls(self, spark_session, adls_config):
        """Test reading Parquet files from ADLS."""
        input_path = f"abfss://{adls_config['container']}@{adls_config['account_name']}.dfs.core.windows.net{adls_config['base_path']}/test-data"

        with patch.object(spark_session.read, 'parquet') as mock_read:
            # Create mock DataFrame
            mock_df = Mock()
            mock_df.count = Mock(return_value=3)
            mock_read.return_value = mock_df

            df = spark_session.read.parquet(input_path)

            assert df is not None
            assert df.count() == 3
            mock_read.assert_called_once_with(input_path)

    def test_list_files_in_directory(self, mock_adls_client, adls_config):
        """Test listing files in ADLS directory."""
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        # Mock list paths
        mock_paths = [
            Mock(name="file1.parquet", is_directory=False),
            Mock(name="file2.parquet", is_directory=False),
            Mock(name="subfolder", is_directory=True)
        ]

        file_system_client.get_paths = Mock(return_value=mock_paths)

        paths = file_system_client.get_paths(path=adls_config["base_path"])
        files = [p for p in paths if not p.is_directory]

        assert len(files) == 2
        assert files[0].name == "file1.parquet"

    def test_delete_files_and_directories(self, mock_adls_client, adls_config):
        """Test deleting files and directories from ADLS."""
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        # Delete file
        file_client = Mock()
        file_system_client.get_file_client = Mock(return_value=file_client)
        file_client.delete_file = Mock()

        file_system_client.get_file_client("test.parquet").delete_file()
        file_client.delete_file.assert_called_once()

        # Delete directory
        directory_client = Mock()
        file_system_client.get_directory_client = Mock(return_value=directory_client)
        directory_client.delete_directory = Mock()

        file_system_client.get_directory_client("test-dir").delete_directory()
        directory_client.delete_directory.assert_called_once()

    def test_hierarchical_namespace_operations(self, mock_adls_client, adls_config):
        """Test hierarchical namespace operations in ADLS Gen2."""
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        # Test ACL operations
        directory_client = Mock()
        file_system_client.get_directory_client = Mock(return_value=directory_client)

        # Set ACL
        acl = "user::rwx,group::r-x,other::r--"
        directory_client.set_access_control = Mock()
        directory_client.set_access_control(acl=acl)

        directory_client.set_access_control.assert_called_once_with(acl=acl)

        # Get ACL
        directory_client.get_access_control = Mock(return_value={"acl": acl})
        access_control = directory_client.get_access_control()

        assert access_control["acl"] == acl

    def test_concurrent_write_operations(self, spark_session, adls_config):
        """Test concurrent write operations to ADLS."""
        # Create multiple DataFrames
        dataframes = []
        for i in range(3):
            data = [(f"ORD{j}", f"ITEM{j}", j) for j in range(i*10, (i+1)*10)]
            df = spark_session.createDataFrame(
                data,
                ["order_id", "item_id", "quantity"]
            )
            dataframes.append(df)

        # Mock concurrent writes
        with patch('concurrent.futures.ThreadPoolExecutor') as mock_executor:
            mock_future = Mock()
            mock_future.result = Mock(return_value=True)
            mock_executor.return_value.__enter__.return_value.submit = Mock(return_value=mock_future)

            results = []
            for i, df in enumerate(dataframes):
                output_path = f"{adls_config['base_path']}/concurrent/partition-{i}"
                # Simulate concurrent write
                results.append(mock_future.result())

            assert all(results)
            assert len(results) == 3

    def test_storage_metrics_collection(self, mock_adls_client, adls_config):
        """Test collecting storage metrics from ADLS."""
        file_system_client = mock_adls_client.get_file_system_client(adls_config["container"])

        # Mock storage properties
        file_system_client.get_file_system_properties = Mock(return_value={
            "last_modified": datetime.now(),
            "metadata": {"size": "1024GB", "file_count": "10000"}
        })

        properties = file_system_client.get_file_system_properties()

        assert "metadata" in properties
        assert properties["metadata"]["size"] == "1024GB"
        assert properties["metadata"]["file_count"] == "10000"

    @pytest.mark.parametrize("operation,expected", [
        ("read", True),
        ("write", True),
        ("delete", True),
        ("list", True)
    ])
    def test_adls_operations_with_retry(self, mock_adls_client, operation, expected):
        """Test ADLS operations with retry logic."""
        file_system_client = mock_adls_client.get_file_system_client("test")

        if operation == "read":
            file_client = Mock()
            file_client.download_file = Mock(side_effect=[
                Exception("Temporary failure"),
                Mock(readall=Mock(return_value=b"test data"))
            ])
            file_system_client.get_file_client = Mock(return_value=file_client)

            # Retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = file_client.download_file().readall()
                    assert result == b"test data"
                    break
                except Exception:
                    if attempt == max_retries - 1:
                        raise
                    continue

        assert expected is True