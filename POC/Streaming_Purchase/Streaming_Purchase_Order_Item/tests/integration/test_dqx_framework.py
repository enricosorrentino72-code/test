"""
Integration tests for DQX Framework.

Tests cover:
- DQX rule execution
- Data quality validation
- Rule chaining and orchestration
- Error handling in DQX pipelines
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add necessary paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utility')))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, when, lit


class TestDQXFrameworkIntegration:
    """Integration tests for DQX Framework."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for integration testing."""
        spark = SparkSession.builder \
            .appName("DQXFrameworkIntegrationTest") \
            .config("spark.master", "local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def sample_data(self, spark_session):
        """Create sample data for DQX testing."""
        data = [
            ("ORD001", "ITEM001", 10, 100.0, "2024-09-19 10:00:00"),
            ("ORD002", "ITEM002", -5, 50.0, "2024-09-19 11:00:00"),  # Invalid quantity
            ("ORD003", None, 3, 30.0, "2024-09-19 12:00:00"),  # Null item_id
            ("ORD004", "ITEM004", 0, 0.0, "2024-09-19 13:00:00"),  # Zero values
            ("ORD005", "ITEM005", 15, 150.0, None),  # Null timestamp
            ("ORD001", "ITEM001", 10, 100.0, "2024-09-19 14:00:00"),  # Duplicate
        ]

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])

        return spark_session.createDataFrame(data, schema)

    @pytest.fixture
    def dqx_rules(self):
        """Define DQX rules for testing."""
        return [
            {
                "name": "not_null_check",
                "columns": ["order_id", "item_id"],
                "type": "completeness"
            },
            {
                "name": "positive_quantity",
                "column": "quantity",
                "condition": "quantity > 0",
                "type": "validity"
            },
            {
                "name": "amount_consistency",
                "condition": "amount >= 0",
                "type": "consistency"
            },
            {
                "name": "duplicate_check",
                "columns": ["order_id", "item_id"],
                "type": "uniqueness"
            },
            {
                "name": "timestamp_format",
                "column": "timestamp",
                "pattern": r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
                "type": "format"
            }
        ]

    def test_dqx_completeness_validation(self, spark_session, sample_data):
        """Test DQX completeness validation."""
        # Check for null values
        null_checks = sample_data.filter(
            col("order_id").isNull() |
            col("item_id").isNull()
        )

        assert null_checks.count() == 1  # One record with null item_id

        # Add DQX flag for completeness
        validated_df = sample_data.withColumn(
            "dqx_completeness_flag",
            when(col("order_id").isNotNull() & col("item_id").isNotNull(), True)
            .otherwise(False)
        )

        passed_records = validated_df.filter(col("dqx_completeness_flag") == True).count()
        assert passed_records == 5

    def test_dqx_validity_validation(self, spark_session, sample_data):
        """Test DQX validity validation."""
        # Check quantity validity
        valid_quantity_df = sample_data.withColumn(
            "dqx_quantity_valid",
            when(col("quantity") > 0, True).otherwise(False)
        )

        invalid_records = valid_quantity_df.filter(col("dqx_quantity_valid") == False)
        assert invalid_records.count() == 2  # One negative, one zero

    def test_dqx_uniqueness_validation(self, spark_session, sample_data):
        """Test DQX uniqueness validation."""
        # Check for duplicates
        duplicate_check = sample_data.groupBy("order_id", "item_id") \
            .count() \
            .filter(col("count") > 1)

        assert duplicate_check.count() == 1  # One duplicate combination

    def test_dqx_consistency_validation(self, spark_session, sample_data):
        """Test DQX consistency validation."""
        # Check amount consistency
        consistency_check = sample_data.withColumn(
            "dqx_amount_consistent",
            when(col("amount") >= 0, True).otherwise(False)
        )

        consistent_records = consistency_check.filter(col("dqx_amount_consistent") == True)
        assert consistent_records.count() == 6  # All records have non-negative amounts

    def test_dqx_format_validation(self, spark_session, sample_data):
        """Test DQX format validation."""
        # Check timestamp format
        format_check = sample_data.withColumn(
            "dqx_timestamp_valid",
            when(col("timestamp").rlike(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), True)
            .otherwise(False)
        )

        invalid_format = format_check.filter(col("dqx_timestamp_valid") == False)
        assert invalid_format.count() == 1  # One null timestamp

    def test_dqx_rule_chaining(self, spark_session, sample_data, dqx_rules):
        """Test chaining multiple DQX rules."""
        result_df = sample_data

        # Apply all rules sequentially
        result_df = result_df.withColumn(
            "dqx_completeness",
            when(col("order_id").isNotNull() & col("item_id").isNotNull(), 1).otherwise(0)
        )

        result_df = result_df.withColumn(
            "dqx_validity",
            when(col("quantity") > 0, 1).otherwise(0)
        )

        result_df = result_df.withColumn(
            "dqx_consistency",
            when(col("amount") >= 0, 1).otherwise(0)
        )

        # Calculate overall DQX score
        result_df = result_df.withColumn(
            "dqx_score",
            (col("dqx_completeness") + col("dqx_validity") + col("dqx_consistency")) / 3
        )

        # Check records that pass all rules
        perfect_quality = result_df.filter(col("dqx_score") == 1.0)
        assert perfect_quality.count() == 3  # Only 3 records pass all rules

    def test_dqx_error_handling(self, spark_session, sample_data):
        """Test DQX error handling and recovery."""
        # Simulate DQX rule execution with error
        try:
            # Invalid column reference should raise error
            sample_data.withColumn(
                "dqx_check",
                when(col("non_existent_column") > 0, True).otherwise(False)
            ).collect()
            assert False, "Should have raised an error"
        except Exception as e:
            assert "non_existent_column" in str(e) or "cannot resolve" in str(e).lower()

    def test_dqx_performance_metrics(self, spark_session, sample_data):
        """Test DQX performance metrics collection."""
        import time

        start_time = time.time()

        # Run multiple DQX validations
        for _ in range(3):
            validated = sample_data.filter(col("order_id").isNotNull())
            validated.count()

        execution_time = time.time() - start_time

        assert execution_time < 5.0  # Should complete within 5 seconds

    def test_dqx_quarantine_process(self, spark_session, sample_data):
        """Test DQX quarantine process for failed records."""
        # Apply DQX rules and separate passed/failed records
        validated_df = sample_data.withColumn(
            "dqx_pass",
            when(
                col("order_id").isNotNull() &
                col("item_id").isNotNull() &
                (col("quantity") > 0) &
                (col("amount") >= 0),
                True
            ).otherwise(False)
        )

        # Separate streams
        passed_df = validated_df.filter(col("dqx_pass") == True)
        quarantine_df = validated_df.filter(col("dqx_pass") == False)

        assert passed_df.count() == 3
        assert quarantine_df.count() == 3

        # Verify quarantine records have issues
        quarantine_records = quarantine_df.collect()
        for record in quarantine_records:
            assert (
                record["item_id"] is None or
                record["quantity"] <= 0 or
                record["amount"] < 0
            )

    def test_dqx_report_generation(self, spark_session, sample_data, dqx_rules):
        """Test DQX report generation."""
        total_records = sample_data.count()

        # Run validations and collect metrics
        metrics = {
            "total_records": total_records,
            "completeness_failures": sample_data.filter(col("item_id").isNull()).count(),
            "validity_failures": sample_data.filter(col("quantity") <= 0).count(),
            "uniqueness_failures": 1,  # Known duplicate
            "format_failures": sample_data.filter(col("timestamp").isNull()).count()
        }

        # Calculate pass rate
        total_failures = sum([v for k, v in metrics.items() if k.endswith("_failures")])
        pass_rate = (total_records - total_failures) / total_records * 100

        # Generate report
        report = {
            "execution_time": datetime.now().isoformat(),
            "metrics": metrics,
            "pass_rate": pass_rate,
            "rules_executed": len(dqx_rules)
        }

        assert report["metrics"]["total_records"] == 6
        assert report["rules_executed"] == 5
        assert report["pass_rate"] < 100  # Some records should fail