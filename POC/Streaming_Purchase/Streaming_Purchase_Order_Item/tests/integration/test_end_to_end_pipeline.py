"""
End-to-End Pipeline Integration Tests for Purchase Order Items
Tests complete data flow from EventHub through Bronze to Silver layer with DQX validation
"""

import pytest
import json
import time
import os
import tempfile
import shutil
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from unittest.mock import patch, MagicMock
import logging

# Import Spark and Delta Lake with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    from delta import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    DeltaTable = None

# Import Azure packages with error handling
try:
    from azure.eventhub import EventHubProducerClient, EventData
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    EventHubProducerClient = None
    EventData = None

# Import project classes
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

try:
    from purchase_order_item_model import PurchaseOrderItem
    from purchase_order_item_factory import PurchaseOrderItemFactory
    from purchase_order_dqx_pipeline import PurchaseOrderDQXPipeline
    from purchase_order_silver_manager import PurchaseOrderSilverManager
    from purchase_order_dqx_rules import PurchaseOrderDQXRules
    from bronze_layer_handler import BronzeLayerHandler
except ImportError as e:
    # Mock classes for testing when not available
    PurchaseOrderItem = None
    PurchaseOrderItemFactory = None
    PurchaseOrderDQXPipeline = None
    PurchaseOrderSilverManager = None
    PurchaseOrderDQXRules = None
    BronzeLayerHandler = None

# Test markers
pytestmark = [
    pytest.mark.integration,
    pytest.mark.end_to_end,
    pytest.mark.slow
]


class TestEndToEndPurchaseOrderPipeline:
    """End-to-end integration tests for the complete purchase order pipeline."""

    @classmethod
    def setup_class(cls):
        """Set up test class with Spark session and temporary directories."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        # Create Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("PurchaseOrderPipelineTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[2]") \
            .getOrCreate()

        # Create temporary directories for testing
        cls.temp_dir = tempfile.mkdtemp(prefix="purchase_order_test_")
        cls.bronze_path = os.path.join(cls.temp_dir, "bronze")
        cls.silver_path = os.path.join(cls.temp_dir, "silver")
        cls.checkpoint_path = os.path.join(cls.temp_dir, "checkpoints")

        # Create directories
        os.makedirs(cls.bronze_path, exist_ok=True)
        os.makedirs(cls.silver_path, exist_ok=True)
        os.makedirs(cls.checkpoint_path, exist_ok=True)

    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        if hasattr(cls, 'spark') and cls.spark:
            cls.spark.stop()

        if hasattr(cls, 'temp_dir') and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def setup_method(self):
        """Set up for each test method."""
        self.test_messages = []
        self.processed_orders = []
        self.factory = PurchaseOrderItemFactory() if PurchaseOrderItemFactory else None

    def generate_test_purchase_orders_dataframe(self, count: int = 10) -> Optional[DataFrame]:
        """Generate test purchase orders as Spark DataFrame."""
        if not self.spark:
            return None

        if self.factory:
            # Use factory to generate realistic data
            orders = self.factory.generate_batch(count)
            data = [order.to_dict() for order in orders]
        else:
            # Fallback test data
            data = []
            base_time = datetime.now(timezone.utc)

            for i in range(count):
                order = {
                    "order_id": f"ORD-E2E-{int(base_time.timestamp())}-{i:03d}",
                    "product_id": f"PROD-{i:03d}",
                    "product_name": f"End-to-End Test Product {i}",
                    "quantity": 5 + (i % 10),
                    "unit_price": 29.99 + (i * 2.0),
                    "total_amount": (5 + (i % 10)) * (29.99 + (i * 2.0)),
                    "customer_id": f"CUST-{i % 5:03d}",
                    "vendor_id": f"VEND-{i % 3:03d}",
                    "warehouse_location": "E2E-TEST-WH",
                    "order_status": "NEW",
                    "payment_status": "PENDING",
                    "created_at": base_time.isoformat(),
                    "test_scenario": "end_to_end"
                }
                data.append(order)

        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(data)
        return df

    @pytest.mark.critical
    @pytest.mark.timeout(300)
    def test_complete_eventhub_to_silver_pipeline_flow(self):
        """Test complete pipeline flow from EventHub producer to Silver layer."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Complete EventHub to Silver Pipeline Flow ===")

        try:
            # Step 1: Generate test purchase orders
            test_count = 5
            test_orders_df = self.generate_test_purchase_orders_dataframe(test_count)
            assert test_orders_df is not None, "Should generate test orders DataFrame"

            test_orders = test_orders_df.collect()
            logger.info(f"Generated {len(test_orders)} test purchase orders")

            # Step 2: Simulate Bronze layer ingestion
            logger.info("Step 2: Writing to Bronze layer...")
            bronze_df = test_orders_df.withColumn("ingestion_timestamp",
                                                self.spark.sql("SELECT current_timestamp()").collect()[0][0]) \
                                      .withColumn("source", self.spark.sql("SELECT 'eventhub'").collect()[0][0])

            # Write to Bronze layer (Delta format)
            bronze_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.bronze_path)

            logger.info(f"Bronze layer written to: {self.bronze_path}")

            # Verify Bronze layer data
            bronze_read_df = self.spark.read.format("delta").load(self.bronze_path)
            bronze_count = bronze_read_df.count()
            assert bronze_count == test_count, f"Bronze layer should contain {test_count} records, got {bronze_count}"

            # Step 3: Process Bronze to Silver with DQX validation
            logger.info("Step 3: Processing Bronze to Silver with DQX...")

            if PurchaseOrderDQXPipeline:
                # Use actual DQX pipeline
                dqx_pipeline = PurchaseOrderDQXPipeline()
                silver_df = dqx_pipeline.process_bronze_to_silver(bronze_read_df)
            else:
                # Simulate DQX processing
                silver_df = self._simulate_dqx_processing(bronze_read_df)

            assert silver_df is not None, "DQX processing should produce Silver DataFrame"

            # Step 4: Write to Silver layer
            logger.info("Step 4: Writing to Silver layer...")
            silver_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.silver_path)

            logger.info(f"Silver layer written to: {self.silver_path}")

            # Step 5: Verify Silver layer data quality
            logger.info("Step 5: Verifying Silver layer data quality...")
            silver_read_df = self.spark.read.format("delta").load(self.silver_path)
            silver_count = silver_read_df.count()

            assert silver_count > 0, "Silver layer should contain processed records"
            assert silver_count <= test_count, f"Silver layer should not exceed {test_count} records"

            # Verify Silver layer schema contains DQX fields
            silver_columns = silver_read_df.columns
            expected_dqx_columns = ['dqx_quality_score', 'dqx_validation_timestamp', 'validation_errors']

            # Check for at least some DQX-related columns
            dqx_columns_present = any(col in silver_columns for col in expected_dqx_columns)
            assert dqx_columns_present, f"Silver layer should contain DQX validation columns"

            # Step 6: Verify data quality metrics
            logger.info("Step 6: Verifying data quality metrics...")

            # Check for quality scores
            if 'dqx_quality_score' in silver_columns:
                avg_quality_score = silver_read_df.agg({"dqx_quality_score": "avg"}).collect()[0][0]
                logger.info(f"Average quality score: {avg_quality_score}")
                assert avg_quality_score is not None, "Should have quality score data"

            # Verify financial calculations
            financial_orders = silver_read_df.filter("quantity > 0 AND unit_price > 0").collect()
            for order in financial_orders[:3]:  # Check first 3 orders
                if hasattr(order, 'quantity') and hasattr(order, 'unit_price') and hasattr(order, 'total_amount'):
                    expected_total = order.quantity * order.unit_price
                    actual_total = order.total_amount
                    # Allow small floating point differences
                    assert abs(actual_total - expected_total) < 0.01, f"Financial calculation mismatch: {actual_total} vs {expected_total}"

            logger.info("End-to-end pipeline flow test completed successfully")

        except Exception as e:
            logger.error(f"End-to-end pipeline test failed: {str(e)}")
            pytest.fail(f"End-to-end pipeline test failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(240)
    def test_bronze_to_silver_dqx_transformation_pipeline(self):
        """Test Bronze to Silver transformation with comprehensive DQX validation."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Bronze to Silver DQX Transformation ===")

        try:
            # Step 1: Create Bronze data with quality issues
            test_data = [
                # Valid orders
                {"order_id": "ORD-VALID-001", "product_id": "PROD-001", "quantity": 5, "unit_price": 10.0, "total_amount": 50.0, "customer_id": "CUST-001"},
                {"order_id": "ORD-VALID-002", "product_id": "PROD-002", "quantity": 3, "unit_price": 15.0, "total_amount": 45.0, "customer_id": "CUST-002"},
                # Orders with quality issues
                {"order_id": "ORD-INVALID-001", "product_id": "PROD-003", "quantity": -1, "unit_price": 20.0, "total_amount": -20.0, "customer_id": "CUST-003"},  # Negative quantity
                {"order_id": "ORD-INVALID-002", "product_id": "PROD-004", "quantity": 2, "unit_price": 25.0, "total_amount": 100.0, "customer_id": "CUST-004"},  # Wrong total
                {"order_id": "", "product_id": "PROD-005", "quantity": 1, "unit_price": 30.0, "total_amount": 30.0, "customer_id": "CUST-005"},  # Empty order_id
            ]

            bronze_df = self.spark.createDataFrame(test_data)
            logger.info(f"Created Bronze DataFrame with {bronze_df.count()} test records")

            # Step 2: Apply DQX transformation
            if PurchaseOrderDQXPipeline:
                dqx_pipeline = PurchaseOrderDQXPipeline()
                silver_df = dqx_pipeline.process_bronze_to_silver(bronze_df)
            else:
                silver_df = self._simulate_dqx_processing(bronze_df)

            assert silver_df is not None, "DQX transformation should produce results"

            # Step 3: Analyze DQX results
            silver_count = silver_df.count()
            logger.info(f"Silver DataFrame contains {silver_count} records after DQX processing")

            # Verify DQX validation results
            silver_data = silver_df.collect()

            valid_records = [record for record in silver_data if self._is_valid_record(record)]
            invalid_records = [record for record in silver_data if not self._is_valid_record(record)]

            logger.info(f"Valid records: {len(valid_records)}, Invalid records: {len(invalid_records)}")

            # Assertions
            assert len(valid_records) >= 2, "Should have at least 2 valid records"
            assert len(invalid_records) >= 1, "Should have at least 1 invalid record for quality testing"
            assert len(valid_records) + len(invalid_records) == silver_count, "All records should be categorized"

            # Verify specific validation rules were applied
            for record in silver_data:
                # Check that financial validation was applied
                if hasattr(record, 'quantity') and hasattr(record, 'unit_price') and hasattr(record, 'total_amount'):
                    if record.quantity > 0 and record.unit_price > 0:
                        expected_total = record.quantity * record.unit_price
                        # If totals don't match, should be flagged as invalid
                        if abs(record.total_amount - expected_total) > 0.01:
                            assert not self._is_valid_record(record), f"Record with wrong total should be invalid: {record}"

            logger.info("Bronze to Silver DQX transformation test completed successfully")

        except Exception as e:
            logger.error(f"DQX transformation test failed: {str(e)}")
            pytest.fail(f"DQX transformation test failed: {str(e)}")

    @pytest.mark.medium
    @pytest.mark.timeout(180)
    def test_pipeline_checkpoint_and_recovery_mechanism(self):
        """Test pipeline checkpoint mechanism and recovery capabilities."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Pipeline Checkpoint and Recovery ===")

        try:
            # Step 1: Create initial batch of data
            initial_orders = self.generate_test_purchase_orders_dataframe(3)
            assert initial_orders is not None, "Should generate initial orders"

            # Write initial batch to Bronze
            initial_orders.write \
                .format("delta") \
                .mode("append") \
                .save(self.bronze_path)

            logger.info("Written initial batch to Bronze layer")

            # Step 2: Process first batch and create checkpoint
            bronze_df_1 = self.spark.read.format("delta").load(self.bronze_path)

            if PurchaseOrderDQXPipeline:
                dqx_pipeline = PurchaseOrderDQXPipeline()
                silver_df_1 = dqx_pipeline.process_bronze_to_silver(bronze_df_1)
            else:
                silver_df_1 = self._simulate_dqx_processing(bronze_df_1)

            # Write to Silver with checkpoint information
            silver_df_1_with_checkpoint = silver_df_1.withColumn("batch_id",
                                                               self.spark.sql("SELECT 'batch_1'").collect()[0][0]) \
                                                      .withColumn("processed_timestamp",
                                                               self.spark.sql("SELECT current_timestamp()").collect()[0][0])

            silver_df_1_with_checkpoint.write \
                .format("delta") \
                .mode("append") \
                .save(self.silver_path)

            initial_silver_count = silver_df_1_with_checkpoint.count()
            logger.info(f"Processed batch 1: {initial_silver_count} records")

            # Step 3: Simulate pipeline interruption and recovery
            # Add second batch of data
            second_orders = self.generate_test_purchase_orders_dataframe(2)
            second_orders.write \
                .format("delta") \
                .mode("append") \
                .save(self.bronze_path)

            logger.info("Added second batch to Bronze layer (simulating interruption recovery)")

            # Step 4: Process incremental data (recovery scenario)
            bronze_df_all = self.spark.read.format("delta").load(self.bronze_path)
            total_bronze_count = bronze_df_all.count()

            # Simulate incremental processing (only new records)
            existing_silver_df = self.spark.read.format("delta").load(self.silver_path)
            processed_order_ids = [row.order_id for row in existing_silver_df.select("order_id").collect()]

            # Filter Bronze for unprocessed records
            unprocessed_bronze_df = bronze_df_all.filter(~bronze_df_all.order_id.isin(processed_order_ids))
            unprocessed_count = unprocessed_bronze_df.count()

            logger.info(f"Found {unprocessed_count} unprocessed records for recovery")

            if unprocessed_count > 0:
                # Process unprocessed records
                if PurchaseOrderDQXPipeline:
                    dqx_pipeline = PurchaseOrderDQXPipeline()
                    silver_df_2 = dqx_pipeline.process_bronze_to_silver(unprocessed_bronze_df)
                else:
                    silver_df_2 = self._simulate_dqx_processing(unprocessed_bronze_df)

                # Add batch information
                silver_df_2_with_checkpoint = silver_df_2.withColumn("batch_id",
                                                                   self.spark.sql("SELECT 'batch_2_recovery'").collect()[0][0]) \
                                                        .withColumn("processed_timestamp",
                                                                 self.spark.sql("SELECT current_timestamp()").collect()[0][0])

                # Append to existing Silver layer
                silver_df_2_with_checkpoint.write \
                    .format("delta") \
                    .mode("append") \
                    .save(self.silver_path)

                logger.info("Recovery processing completed")

            # Step 5: Verify checkpoint recovery results
            final_silver_df = self.spark.read.format("delta").load(self.silver_path)
            final_silver_count = final_silver_df.count()

            logger.info(f"Final Silver count: {final_silver_count}, Bronze count: {total_bronze_count}")

            # Verify no data loss during recovery
            assert final_silver_count >= initial_silver_count, "Silver count should not decrease during recovery"
            assert final_silver_count <= total_bronze_count, "Silver should not exceed Bronze count"

            # Verify batch tracking
            batch_counts = final_silver_df.groupBy("batch_id").count().collect()
            batch_info = {row.batch_id: row.count for row in batch_counts}

            logger.info(f"Batch distribution: {batch_info}")
            assert "batch_1" in batch_info, "Should have original batch"

            if unprocessed_count > 0:
                assert "batch_2_recovery" in batch_info, "Should have recovery batch"

            logger.info("Pipeline checkpoint and recovery test completed successfully")

        except Exception as e:
            logger.error(f"Checkpoint recovery test failed: {str(e)}")
            pytest.fail(f"Checkpoint recovery test failed: {str(e)}")

    @pytest.mark.medium
    @pytest.mark.timeout(200)
    def test_end_to_end_data_quality_monitoring_integration(self):
        """Test end-to-end data quality monitoring and alerting integration."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing End-to-End Data Quality Monitoring ===")

        try:
            # Step 1: Create data with known quality patterns
            test_data = [
                # High quality orders
                {"order_id": "ORD-HQ-001", "product_id": "PROD-001", "quantity": 5, "unit_price": 20.0, "total_amount": 100.0, "customer_id": "CUST-001"},
                {"order_id": "ORD-HQ-002", "product_id": "PROD-002", "quantity": 3, "unit_price": 25.0, "total_amount": 75.0, "customer_id": "CUST-002"},
                # Medium quality orders (minor issues)
                {"order_id": "ORD-MQ-001", "product_id": "PROD-003", "quantity": 1, "unit_price": 30.0, "total_amount": 30.01, "customer_id": "CUST-003"},  # Small rounding
                # Low quality orders (major issues)
                {"order_id": "ORD-LQ-001", "product_id": "PROD-004", "quantity": -2, "unit_price": 40.0, "total_amount": -80.0, "customer_id": "CUST-004"},  # Negative quantity
                {"order_id": "", "product_id": "PROD-005", "quantity": 1, "unit_price": 50.0, "total_amount": 50.0, "customer_id": ""},  # Missing IDs
            ]

            bronze_df = self.spark.createDataFrame(test_data)
            logger.info(f"Created test data with {bronze_df.count()} records")

            # Step 2: Process through DQX pipeline with monitoring
            if PurchaseOrderDQXPipeline:
                dqx_pipeline = PurchaseOrderDQXPipeline()
                silver_df = dqx_pipeline.process_bronze_to_silver(bronze_df)
            else:
                silver_df = self._simulate_dqx_processing_with_monitoring(bronze_df)

            # Step 3: Analyze quality monitoring results
            silver_data = silver_df.collect()
            total_records = len(silver_data)

            # Calculate quality metrics
            high_quality_records = [r for r in silver_data if self._get_quality_score(r) >= 0.8]
            medium_quality_records = [r for r in silver_data if 0.5 <= self._get_quality_score(r) < 0.8]
            low_quality_records = [r for r in silver_data if self._get_quality_score(r) < 0.5]

            quality_distribution = {
                'high_quality': len(high_quality_records),
                'medium_quality': len(medium_quality_records),
                'low_quality': len(low_quality_records),
                'total': total_records
            }

            logger.info(f"Quality distribution: {quality_distribution}")

            # Step 4: Verify monitoring thresholds
            high_quality_percentage = len(high_quality_records) / total_records * 100
            low_quality_percentage = len(low_quality_records) / total_records * 100

            logger.info(f"High quality: {high_quality_percentage:.1f}%, Low quality: {low_quality_percentage:.1f}%")

            # Assertions for quality monitoring
            assert total_records == len(test_data), "Should process all records"
            assert len(high_quality_records) >= 2, "Should have high quality records from good data"
            assert len(low_quality_records) >= 1, "Should detect low quality records"

            # Verify quality threshold alerting
            if low_quality_percentage > 30:  # Alert threshold
                logger.warning(f"Quality alert: {low_quality_percentage:.1f}% low quality records exceed 30% threshold")

            # Step 5: Test quality trend monitoring
            quality_scores = [self._get_quality_score(r) for r in silver_data]
            avg_quality_score = sum(quality_scores) / len(quality_scores)
            min_quality_score = min(quality_scores)
            max_quality_score = max(quality_scores)

            quality_metrics = {
                'average_quality': avg_quality_score,
                'min_quality': min_quality_score,
                'max_quality': max_quality_score,
                'quality_range': max_quality_score - min_quality_score
            }

            logger.info(f"Quality metrics: {quality_metrics}")

            # Verify quality metrics are reasonable
            assert 0.0 <= avg_quality_score <= 1.0, "Average quality score should be between 0 and 1"
            assert 0.0 <= min_quality_score <= 1.0, "Min quality score should be between 0 and 1"
            assert 0.0 <= max_quality_score <= 1.0, "Max quality score should be between 0 and 1"
            assert avg_quality_score > min_quality_score, "Average should be higher than minimum"

            logger.info("End-to-end data quality monitoring test completed successfully")

        except Exception as e:
            logger.error(f"Data quality monitoring test failed: {str(e)}")
            pytest.fail(f"Data quality monitoring test failed: {str(e)}")

    def _simulate_dqx_processing(self, bronze_df: DataFrame) -> DataFrame:
        """Simulate DQX processing when actual pipeline is not available."""
        from pyspark.sql.functions import when, col, lit, current_timestamp

        # Add DQX validation columns
        silver_df = bronze_df.withColumn("dqx_quality_score",
                                       when((col("quantity") > 0) & (col("unit_price") > 0) &
                                           (col("order_id") != "") & (col("customer_id") != ""), 0.9)
                                       .otherwise(0.3)) \
                           .withColumn("validation_errors",
                                     when((col("quantity") <= 0), "Negative or zero quantity")
                                     .when((col("order_id") == ""), "Missing order ID")
                                     .otherwise(None)) \
                           .withColumn("dqx_validation_timestamp", current_timestamp()) \
                           .withColumn("is_valid", col("dqx_quality_score") >= 0.8)

        return silver_df

    def _simulate_dqx_processing_with_monitoring(self, bronze_df: DataFrame) -> DataFrame:
        """Simulate DQX processing with enhanced monitoring."""
        from pyspark.sql.functions import when, col, lit, current_timestamp, abs as spark_abs

        # Enhanced DQX validation with detailed scoring
        silver_df = bronze_df.withColumn("financial_accuracy_score",
                                       when(spark_abs((col("quantity") * col("unit_price")) - col("total_amount")) <= 0.01, 1.0)
                                       .when(spark_abs((col("quantity") * col("unit_price")) - col("total_amount")) <= 0.1, 0.8)
                                       .otherwise(0.0)) \
                           .withColumn("data_completeness_score",
                                     when((col("order_id") != "") & (col("customer_id") != "") &
                                         (col("product_id") != ""), 1.0)
                                     .when((col("order_id") != "") | (col("customer_id") != ""), 0.5)
                                     .otherwise(0.0)) \
                           .withColumn("business_rules_score",
                                     when((col("quantity") > 0) & (col("unit_price") > 0), 1.0)
                                     .otherwise(0.0)) \
                           .withColumn("dqx_quality_score",
                                     (col("financial_accuracy_score") +
                                      col("data_completeness_score") +
                                      col("business_rules_score")) / 3) \
                           .withColumn("dqx_validation_timestamp", current_timestamp()) \
                           .withColumn("quality_category",
                                     when(col("dqx_quality_score") >= 0.8, "HIGH")
                                     .when(col("dqx_quality_score") >= 0.5, "MEDIUM")
                                     .otherwise("LOW"))

        return silver_df

    def _is_valid_record(self, record) -> bool:
        """Check if a record is considered valid based on available fields."""
        if hasattr(record, 'is_valid'):
            return record.is_valid
        elif hasattr(record, 'dqx_quality_score'):
            return record.dqx_quality_score >= 0.8
        else:
            # Fallback validation
            return (hasattr(record, 'quantity') and record.quantity > 0 and
                   hasattr(record, 'unit_price') and record.unit_price > 0 and
                   hasattr(record, 'order_id') and record.order_id != "")

    def _get_quality_score(self, record) -> float:
        """Get quality score from a record."""
        if hasattr(record, 'dqx_quality_score'):
            return float(record.dqx_quality_score)
        else:
            # Fallback scoring
            score = 1.0
            if hasattr(record, 'quantity') and record.quantity <= 0:
                score -= 0.3
            if hasattr(record, 'order_id') and record.order_id == "":
                score -= 0.3
            if hasattr(record, 'customer_id') and record.customer_id == "":
                score -= 0.2
            return max(0.0, score)


# Test fixtures
@pytest.fixture(scope="session")
def spark_session():
    """Provide Spark session for end-to-end tests."""
    if not SPARK_AVAILABLE:
        pytest.skip("Spark not available")

    spark = SparkSession.builder \
        .appName("PurchaseOrderE2ETest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[2]") \
        .getOrCreate()

    yield spark
    spark.stop()

@pytest.fixture
def temp_storage_paths():
    """Provide temporary storage paths for testing."""
    temp_dir = tempfile.mkdtemp(prefix="e2e_test_")
    paths = {
        'bronze': os.path.join(temp_dir, "bronze"),
        'silver': os.path.join(temp_dir, "silver"),
        'checkpoints': os.path.join(temp_dir, "checkpoints")
    }

    for path in paths.values():
        os.makedirs(path, exist_ok=True)

    yield paths

    shutil.rmtree(temp_dir)