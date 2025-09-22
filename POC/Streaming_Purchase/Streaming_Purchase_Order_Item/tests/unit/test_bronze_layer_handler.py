"""
Unit tests for BronzeLayerHandler class.

Tests cover:
- ADLS Gen2 configuration and mounting
- Partition management
- Data validation
- Storage optimization
- Path management
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta
import sys
import os

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from bronze_layer_handler import BronzeLayerHandler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class TestBronzeLayerHandler:
    """Test suite for BronzeLayerHandler class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.conf = Mock()
        spark.conf.set = Mock()
        spark.sql = Mock()
        spark.catalog = Mock()
        spark.catalog.listTables = Mock(return_value=[])
        return spark

    @pytest.fixture
    def bronze_handler(self, mock_spark):
        """Create a BronzeLayerHandler instance for testing."""
        return BronzeLayerHandler(
            spark=mock_spark,
            storage_account="testaccount",
            container="testcontainer",
            bronze_path="/mnt/bronze",
            log_level="INFO"
        )

    def test_initialization(self, bronze_handler):
        """Test BronzeLayerHandler initialization."""
        assert bronze_handler.storage_account == "testaccount"
        assert bronze_handler.container == "testcontainer"
        assert bronze_handler.bronze_path == "/mnt/bronze"
        assert bronze_handler.logger is not None

    def test_configure_adls(self, bronze_handler, mock_spark):
        """Test ADLS configuration."""
        bronze_handler.configure_adls("test-key")

        expected_calls = [
            call("fs.azure.account.auth.type.testaccount.dfs.core.windows.net", "SharedKey"),
            call("fs.azure.account.key.testaccount.dfs.core.windows.net", "test-key")
        ]
        mock_spark.conf.set.assert_has_calls(expected_calls, any_order=True)

    def test_mount_adls(self, bronze_handler):
        """Test ADLS mounting functionality."""
        with patch('dbutils.fs.mount') as mock_mount:
            with patch('dbutils.fs.mounts') as mock_mounts:
                mock_mounts.return_value = []

                result = bronze_handler.mount_adls(
                    mount_point="/mnt/bronze",
                    storage_key="test-key"
                )

                assert result is True
                mock_mount.assert_called_once()

    def test_get_partition_path(self, bronze_handler):
        """Test partition path generation."""
        test_date = datetime(2024, 9, 19, 10, 30, 0)

        path = bronze_handler.get_partition_path(test_date)

        assert "year=2024" in path
        assert "month=09" in path
        assert "day=19" in path
        assert "hour=10" in path

    def test_validate_data_quality(self, bronze_handler, mock_spark):
        """Test data quality validation."""
        # Create mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_df.count = Mock(return_value=100)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.select = Mock(return_value=mock_df)
        mock_df.agg = Mock(return_value=Mock(collect=Mock(return_value=[Mock(asDict=Mock(return_value={'nulls': 5}))])))

        result = bronze_handler.validate_data_quality(mock_df)

        assert result['total_records'] == 100
        assert 'null_counts' in result
        assert result['validation_timestamp'] is not None

    def test_optimize_storage(self, bronze_handler, mock_spark):
        """Test storage optimization."""
        mock_spark.sql.return_value = Mock()

        result = bronze_handler.optimize_storage("/mnt/bronze/data")

        assert result is True
        mock_spark.sql.assert_called()

    def test_write_bronze_data(self, bronze_handler):
        """Test writing data to Bronze layer."""
        mock_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_df.write = mock_writer
        mock_writer.mode = Mock(return_value=mock_writer)
        mock_writer.option = Mock(return_value=mock_writer)
        mock_writer.partitionBy = Mock(return_value=mock_writer)
        mock_writer.parquet = Mock()

        result = bronze_handler.write_bronze_data(
            df=mock_df,
            path="/mnt/bronze/data",
            partition_cols=["year", "month", "day"]
        )

        assert result is True
        mock_writer.mode.assert_called_with("append")
        mock_writer.partitionBy.assert_called_with("year", "month", "day")

    def test_read_bronze_data(self, bronze_handler, mock_spark):
        """Test reading data from Bronze layer."""
        mock_df = Mock(spec=DataFrame)
        mock_spark.read.format = Mock(return_value=Mock(
            option=Mock(return_value=Mock(
                load=Mock(return_value=mock_df)
            ))
        ))

        result = bronze_handler.read_bronze_data("/mnt/bronze/data")

        assert result == mock_df
        mock_spark.read.format.assert_called_with("parquet")

    def test_cleanup_old_partitions(self, bronze_handler):
        """Test cleanup of old partitions."""
        with patch('dbutils.fs.ls') as mock_ls:
            with patch('dbutils.fs.rm') as mock_rm:
                mock_ls.return_value = [
                    Mock(path="/mnt/bronze/year=2024/month=01/day=01/"),
                    Mock(path="/mnt/bronze/year=2024/month=09/day=19/")
                ]

                result = bronze_handler.cleanup_old_partitions(
                    base_path="/mnt/bronze",
                    retention_days=30
                )

                assert result['partitions_removed'] >= 0
                assert result['status'] == 'success'

    def test_get_storage_metrics(self, bronze_handler):
        """Test retrieval of storage metrics."""
        with patch('dbutils.fs.ls') as mock_ls:
            mock_ls.return_value = [
                Mock(size=1024000, path="/file1.parquet"),
                Mock(size=2048000, path="/file2.parquet")
            ]

            metrics = bronze_handler.get_storage_metrics("/mnt/bronze")

            assert metrics['total_size_bytes'] > 0
            assert metrics['file_count'] == 2
            assert 'path' in metrics

    def test_validate_schema(self, bronze_handler):
        """Test schema validation."""
        mock_df = Mock(spec=DataFrame)
        mock_df.schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        expected_schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        result = bronze_handler.validate_schema(mock_df, expected_schema)

        assert result is True

    def test_error_handling_write_failure(self, bronze_handler):
        """Test error handling for write failures."""
        mock_df = Mock(spec=DataFrame)
        mock_df.write = Mock(side_effect=Exception("Write failed"))

        with pytest.raises(Exception) as exc_info:
            bronze_handler.write_bronze_data(mock_df, "/mnt/bronze/data")

        assert "Write failed" in str(exc_info.value)