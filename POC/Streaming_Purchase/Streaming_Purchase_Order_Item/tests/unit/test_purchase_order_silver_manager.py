"""
Unit tests for PurchaseOrderSilverManager class.

Tests cover:
- Silver layer data processing
- Data transformation and enrichment
- Schema management
- Data quality enforcement
- Performance optimization
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import sys
import os

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from purchase_order_silver_manager import PurchaseOrderSilverManager
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit


class TestPurchaseOrderSilverManager:
    """Test suite for PurchaseOrderSilverManager class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.sql = Mock()
        spark.read = Mock()
        spark.createDataFrame = Mock()
        return spark

    @pytest.fixture
    def silver_manager(self, mock_spark):
        """Create a PurchaseOrderSilverManager instance for testing."""
        return PurchaseOrderSilverManager(
            spark=mock_spark,
            silver_path="/mnt/silver",
            database="purchase_order_db",
            table="silver_purchase_orders",
            log_level="INFO"
        )

    def test_initialization(self, silver_manager):
        """Test PurchaseOrderSilverManager initialization."""
        assert silver_manager.silver_path == "/mnt/silver"
        assert silver_manager.database == "purchase_order_db"
        assert silver_manager.table == "silver_purchase_orders"
        assert silver_manager.logger is not None

    def test_transform_bronze_to_silver(self, silver_manager):
        """Test bronze to silver transformation."""
        mock_bronze_df = Mock(spec=DataFrame)
        mock_bronze_df.withColumn = Mock(return_value=mock_bronze_df)
        mock_bronze_df.filter = Mock(return_value=mock_bronze_df)
        mock_bronze_df.drop = Mock(return_value=mock_bronze_df)
        mock_bronze_df.select = Mock(return_value=mock_bronze_df)

        result = silver_manager.transform_bronze_to_silver(mock_bronze_df)

        assert result is not None
        mock_bronze_df.withColumn.assert_called()
        mock_bronze_df.filter.assert_called()

    def test_apply_business_rules(self, silver_manager):
        """Test applying business rules."""
        mock_df = Mock(spec=DataFrame)
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)

        business_rules = [
            {'type': 'filter', 'condition': 'quantity > 0'},
            {'type': 'transform', 'column': 'total_amount', 'formula': 'quantity * unit_price'}
        ]

        result = silver_manager.apply_business_rules(mock_df, business_rules)

        assert result is not None
        assert mock_df.filter.call_count >= 1

    def test_enrich_with_master_data(self, silver_manager, mock_spark):
        """Test data enrichment with master data."""
        mock_main_df = Mock(spec=DataFrame)
        mock_master_df = Mock(spec=DataFrame)
        mock_main_df.join = Mock(return_value=mock_main_df)

        mock_spark.read.format = Mock(return_value=Mock(
            load=Mock(return_value=mock_master_df)
        ))

        result = silver_manager.enrich_with_master_data(
            df=mock_main_df,
            master_table="product_master",
            join_keys=["product_id"]
        )

        assert result is not None
        mock_main_df.join.assert_called_once()

    def test_deduplicate_records(self, silver_manager):
        """Test record deduplication."""
        mock_df = Mock(spec=DataFrame)
        mock_df.dropDuplicates = Mock(return_value=mock_df)
        mock_df.orderBy = Mock(return_value=Mock(
            dropDuplicates=Mock(return_value=mock_df)
        ))

        result = silver_manager.deduplicate_records(
            df=mock_df,
            key_columns=["order_id", "line_item_id"],
            order_column="timestamp"
        )

        assert result is not None
        mock_df.orderBy.assert_called()

    def test_write_silver_data(self, silver_manager):
        """Test writing data to silver layer."""
        mock_df = Mock(spec=DataFrame)
        mock_writer = Mock()
        mock_df.write = mock_writer
        mock_writer.mode = Mock(return_value=mock_writer)
        mock_writer.option = Mock(return_value=mock_writer)
        mock_writer.partitionBy = Mock(return_value=mock_writer)
        mock_writer.saveAsTable = Mock()

        result = silver_manager.write_silver_data(
            df=mock_df,
            mode="append",
            partition_columns=["year", "month"]
        )

        assert result is True
        mock_writer.mode.assert_called_with("append")
        mock_writer.partitionBy.assert_called_with("year", "month")

    def test_create_silver_schema(self, silver_manager):
        """Test silver schema creation."""
        schema = silver_manager.create_silver_schema()

        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0

        # Check for essential fields
        field_names = [field.name for field in schema.fields]
        assert "order_id" in field_names
        assert "processed_timestamp" in field_names

    def test_validate_silver_data(self, silver_manager):
        """Test silver data validation."""
        mock_df = Mock(spec=DataFrame)
        mock_df.filter = Mock(return_value=Mock(count=Mock(return_value=0)))
        mock_df.count = Mock(return_value=1000)

        validations = [
            {'column': 'order_id', 'check': 'not_null'},
            {'column': 'amount', 'check': 'positive'},
            {'column': 'status', 'check': 'valid_values', 'values': ['NEW', 'PROCESSED', 'CANCELLED']}
        ]

        result = silver_manager.validate_silver_data(mock_df, validations)

        assert result['is_valid'] is True
        assert result['total_records'] == 1000
        assert result['validation_errors'] == []

    def test_calculate_aggregations(self, silver_manager):
        """Test aggregation calculations."""
        mock_df = Mock(spec=DataFrame)
        mock_df.groupBy = Mock(return_value=Mock(
            agg=Mock(return_value=mock_df)
        ))

        aggregations = {
            'total_amount': 'sum',
            'order_count': 'count',
            'avg_quantity': 'avg'
        }

        result = silver_manager.calculate_aggregations(
            df=mock_df,
            group_columns=['product_id', 'store_id'],
            aggregations=aggregations
        )

        assert result is not None
        mock_df.groupBy.assert_called_once()

    def test_merge_incremental_updates(self, silver_manager, mock_spark):
        """Test merging incremental updates."""
        mock_target_df = Mock(spec=DataFrame)
        mock_source_df = Mock(spec=DataFrame)

        with patch('delta.tables.DeltaTable') as mock_delta:
            mock_delta_table = Mock()
            mock_delta.forPath = Mock(return_value=mock_delta_table)
            mock_delta_table.merge = Mock(return_value=Mock(
                whenMatchedUpdateAll=Mock(return_value=Mock(
                    whenNotMatchedInsertAll=Mock(return_value=Mock(
                        execute=Mock()
                    ))
                ))
            ))

            result = silver_manager.merge_incremental_updates(
                source_df=mock_source_df,
                merge_keys=["order_id"]
            )

            assert result is True

    def test_optimize_silver_table(self, silver_manager, mock_spark):
        """Test silver table optimization."""
        result = silver_manager.optimize_silver_table(
            zorder_columns=["order_date", "store_id"]
        )

        assert result is True
        mock_spark.sql.assert_called()

    def test_get_silver_metrics(self, silver_manager, mock_spark):
        """Test retrieving silver layer metrics."""
        mock_result = Mock()
        mock_result.collect = Mock(return_value=[
            Mock(asDict=Mock(return_value={
                'record_count': 10000,
                'distinct_orders': 5000,
                'total_amount': 1000000.00
            }))
        ])
        mock_spark.sql.return_value = mock_result

        metrics = silver_manager.get_silver_metrics()

        assert metrics is not None
        assert metrics['record_count'] == 10000
        assert metrics['distinct_orders'] == 5000