"""
Unit tests for HiveMetastoreManager class.

Tests cover:
- Database and table management
- Schema evolution
- Partitioning strategies
- Metadata operations
- Performance optimization
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import sys
import os

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from hive_metastore_manager import HiveMetastoreManager
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class TestHiveMetastoreManager:
    """Test suite for HiveMetastoreManager class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.sql = Mock()
        spark.catalog = Mock()
        spark.catalog.listDatabases = Mock(return_value=['default', 'test_db'])
        spark.catalog.listTables = Mock(return_value=[])
        spark.catalog.tableExists = Mock(return_value=False)
        spark.catalog.databaseExists = Mock(return_value=False)
        spark.table = Mock()
        return spark

    @pytest.fixture
    def metastore_manager(self, mock_spark):
        """Create a HiveMetastoreManager instance for testing."""
        return HiveMetastoreManager(
            spark=mock_spark,
            database="purchase_order_db",
            log_level="INFO"
        )

    def test_initialization(self, metastore_manager):
        """Test HiveMetastoreManager initialization."""
        assert metastore_manager.database == "purchase_order_db"
        assert metastore_manager.logger is not None
        assert metastore_manager.spark is not None

    def test_create_database(self, metastore_manager, mock_spark):
        """Test database creation."""
        result = metastore_manager.create_database(
            database_name="test_db",
            location="/mnt/hive/test_db"
        )

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "CREATE DATABASE IF NOT EXISTS" in call_args
        assert "test_db" in call_args

    def test_create_managed_table(self, metastore_manager, mock_spark):
        """Test managed table creation."""
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        result = metastore_manager.create_managed_table(
            table_name="test_table",
            schema=schema,
            partition_columns=["year", "month"]
        )

        assert result is True
        mock_spark.sql.assert_called()

    def test_create_external_table(self, metastore_manager, mock_spark):
        """Test external table creation."""
        result = metastore_manager.create_external_table(
            table_name="external_table",
            location="/mnt/data/external",
            format="parquet"
        )

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "USING parquet" in call_args
        assert "LOCATION" in call_args

    def test_register_delta_table(self, metastore_manager, mock_spark):
        """Test Delta table registration."""
        result = metastore_manager.register_delta_table(
            table_name="delta_table",
            path="/mnt/delta/table"
        )

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert "USING DELTA" in call_args

    def test_add_table_partition(self, metastore_manager, mock_spark):
        """Test adding table partitions."""
        result = metastore_manager.add_table_partition(
            table_name="partitioned_table",
            partition_spec={"year": "2024", "month": "09"},
            location="/mnt/data/year=2024/month=09"
        )

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "ALTER TABLE" in call_args
        assert "ADD IF NOT EXISTS PARTITION" in call_args

    def test_refresh_table_metadata(self, metastore_manager, mock_spark):
        """Test table metadata refresh."""
        result = metastore_manager.refresh_table_metadata("test_table")

        assert result is True
        expected_calls = [
            call("REFRESH TABLE purchase_order_db.test_table"),
            call("ANALYZE TABLE purchase_order_db.test_table COMPUTE STATISTICS")
        ]
        assert mock_spark.sql.call_count >= 2

    def test_get_table_schema(self, metastore_manager, mock_spark):
        """Test retrieving table schema."""
        mock_df = Mock(spec=DataFrame)
        mock_df.schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", IntegerType(), True)
        ])
        mock_spark.table.return_value = mock_df

        schema = metastore_manager.get_table_schema("test_table")

        assert schema is not None
        assert len(schema.fields) == 2
        mock_spark.table.assert_called_with("purchase_order_db.test_table")

    def test_table_exists(self, metastore_manager, mock_spark):
        """Test checking if table exists."""
        mock_spark.catalog.tableExists.return_value = True

        result = metastore_manager.table_exists("existing_table")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_with("purchase_order_db.existing_table")

    def test_drop_table(self, metastore_manager, mock_spark):
        """Test dropping a table."""
        result = metastore_manager.drop_table("old_table", purge=True)

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "DROP TABLE IF EXISTS" in call_args
        assert "PURGE" in call_args

    def test_optimize_table(self, metastore_manager, mock_spark):
        """Test table optimization."""
        result = metastore_manager.optimize_table(
            table_name="delta_table",
            zorder_columns=["id", "timestamp"]
        )

        assert result is True
        # Should have called OPTIMIZE and ZORDER
        assert mock_spark.sql.call_count >= 1

    def test_vacuum_table(self, metastore_manager, mock_spark):
        """Test table vacuum operation."""
        result = metastore_manager.vacuum_table(
            table_name="delta_table",
            retention_hours=168
        )

        assert result is True
        mock_spark.sql.assert_called()
        call_args = mock_spark.sql.call_args[0][0]
        assert "VACUUM" in call_args
        assert "RETAIN 168 HOURS" in call_args

    def test_get_table_statistics(self, metastore_manager, mock_spark):
        """Test retrieving table statistics."""
        mock_result = Mock()
        mock_result.collect = Mock(return_value=[
            Mock(asDict=Mock(return_value={'num_rows': 1000, 'total_size': 50000}))
        ])
        mock_spark.sql.return_value = mock_result

        stats = metastore_manager.get_table_statistics("test_table")

        assert stats is not None
        assert 'num_rows' in stats
        assert stats['num_rows'] == 1000

    def test_error_handling_table_creation(self, metastore_manager, mock_spark):
        """Test error handling during table creation."""
        mock_spark.sql.side_effect = Exception("Table creation failed")

        with pytest.raises(Exception) as exc_info:
            metastore_manager.create_managed_table(
                table_name="error_table",
                schema=StructType([])
            )

        assert "Table creation failed" in str(exc_info.value)