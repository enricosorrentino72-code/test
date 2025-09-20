"""
Unit tests for PurchaseOrderDQXPipeline class.

Tests cover:
- Pipeline initialization and configuration
- DQX rule execution
- Data quality validation
- Pipeline orchestration
- Error handling and recovery
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import sys
import os

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from purchase_order_dqx_pipeline import PurchaseOrderDQXPipeline
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class TestPurchaseOrderDQXPipeline:
    """Test suite for PurchaseOrderDQXPipeline class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.sql = Mock()
        spark.readStream = Mock()
        spark.read = Mock()
        return spark

    @pytest.fixture
    def dqx_pipeline(self, mock_spark):
        """Create a PurchaseOrderDQXPipeline instance for testing."""
        return PurchaseOrderDQXPipeline(
            spark=mock_spark,
            bronze_path="/mnt/bronze",
            silver_path="/mnt/silver",
            checkpoint_path="/mnt/checkpoints",
            log_level="INFO"
        )

    def test_initialization(self, dqx_pipeline):
        """Test PurchaseOrderDQXPipeline initialization."""
        assert dqx_pipeline.bronze_path == "/mnt/bronze"
        assert dqx_pipeline.silver_path == "/mnt/silver"
        assert dqx_pipeline.checkpoint_path == "/mnt/checkpoints"
        assert dqx_pipeline.logger is not None

    def test_configure_dqx_rules(self, dqx_pipeline):
        """Test DQX rules configuration."""
        rules = [
            {'name': 'not_null', 'column': 'id'},
            {'name': 'unique', 'column': 'id'},
            {'name': 'range', 'column': 'quantity', 'min': 0, 'max': 1000}
        ]

        result = dqx_pipeline.configure_dqx_rules(rules)

        assert result is True
        assert len(dqx_pipeline.dqx_rules) == 3
        assert dqx_pipeline.dqx_rules[0]['name'] == 'not_null'

    def test_start_streaming_pipeline(self, dqx_pipeline, mock_spark):
        """Test starting the streaming pipeline."""
        mock_stream = Mock()
        mock_spark.readStream.format = Mock(return_value=Mock(
            option=Mock(return_value=Mock(
                schema=Mock(return_value=Mock(
                    load=Mock(return_value=mock_stream)
                ))
            ))
        ))

        result = dqx_pipeline.start_streaming_pipeline(
            source_format="parquet",
            max_files_per_trigger=10
        )

        assert result is not None
        mock_spark.readStream.format.assert_called_with("parquet")

    def test_apply_dqx_validation(self, dqx_pipeline):
        """Test applying DQX validation rules."""
        mock_df = Mock(spec=DataFrame)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.withColumn = Mock(return_value=mock_df)

        # Configure rules first
        dqx_pipeline.dqx_rules = [
            {'name': 'not_null', 'column': 'id'},
            {'name': 'unique', 'column': 'id'}
        ]

        result = dqx_pipeline.apply_dqx_validation(mock_df)

        assert result is not None
        mock_df.withColumn.assert_called()

    def test_process_batch(self, dqx_pipeline):
        """Test batch processing."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count = Mock(return_value=1000)
        mock_df.filter = Mock(return_value=mock_df)
        mock_df.write = Mock()

        result = dqx_pipeline.process_batch(
            batch_df=mock_df,
            batch_id=1
        )

        assert result is True
        mock_df.count.assert_called()

    def test_handle_dqx_failures(self, dqx_pipeline):
        """Test handling DQX failures."""
        mock_failed_df = Mock(spec=DataFrame)
        mock_failed_df.count = Mock(return_value=50)
        mock_failed_df.write = Mock()

        result = dqx_pipeline.handle_dqx_failures(
            failed_df=mock_failed_df,
            failure_path="/mnt/dqx_failures"
        )

        assert result is True
        mock_failed_df.write.assert_called()

    def test_create_quality_report(self, dqx_pipeline):
        """Test quality report creation."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count = Mock(return_value=1000)
        mock_df.filter = Mock(side_effect=[
            Mock(count=Mock(return_value=950)),  # Passed records
            Mock(count=Mock(return_value=50))     # Failed records
        ])

        report = dqx_pipeline.create_quality_report(mock_df)

        assert report['total_records'] == 1000
        assert report['passed_records'] == 950
        assert report['failed_records'] == 50
        assert report['pass_rate'] == 0.95

    def test_checkpoint_management(self, dqx_pipeline):
        """Test checkpoint management."""
        result = dqx_pipeline.save_checkpoint(
            checkpoint_name="test_checkpoint",
            metadata={'batch_id': 1, 'timestamp': datetime.now()}
        )

        assert result is True

        loaded = dqx_pipeline.load_checkpoint("test_checkpoint")
        assert loaded is not None
        assert loaded['batch_id'] == 1

    def test_pipeline_recovery(self, dqx_pipeline, mock_spark):
        """Test pipeline recovery from failure."""
        with patch('os.path.exists', return_value=True):
            result = dqx_pipeline.recover_from_failure(
                last_checkpoint="/mnt/checkpoints/last"
            )

            assert result is True

    def test_optimize_pipeline_performance(self, dqx_pipeline, mock_spark):
        """Test pipeline performance optimization."""
        metrics = {
            'processing_time': 100,
            'throughput': 1000,
            'memory_usage': 0.75
        }

        optimizations = dqx_pipeline.optimize_performance(metrics)

        assert optimizations is not None
        assert 'recommendations' in optimizations

    def test_validate_silver_output(self, dqx_pipeline):
        """Test silver layer output validation."""
        mock_df = Mock(spec=DataFrame)
        expected_schema = StructType([
            StructField("id", StringType(), False),
            StructField("quantity", IntegerType(), True)
        ])
        mock_df.schema = expected_schema

        result = dqx_pipeline.validate_silver_output(
            df=mock_df,
            expected_schema=expected_schema
        )

        assert result is True

    def test_pipeline_monitoring(self, dqx_pipeline):
        """Test pipeline monitoring metrics."""
        metrics = dqx_pipeline.get_pipeline_metrics()

        assert metrics is not None
        assert 'uptime' in metrics
        assert 'batches_processed' in metrics
        assert 'current_status' in metrics

    def test_error_handling(self, dqx_pipeline):
        """Test comprehensive error handling."""
        with pytest.raises(ValueError) as exc_info:
            dqx_pipeline.configure_dqx_rules([
                {'name': 'invalid_rule'}  # Missing required fields
            ])

        assert "Invalid rule configuration" in str(exc_info.value)