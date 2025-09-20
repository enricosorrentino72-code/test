"""
Performance tests for DQX framework operations.

Tests cover:
- DQX rule execution performance
- Memory usage during validation
- Scalability testing
- Throughput benchmarks
- Resource utilization monitoring
"""

import pytest
import time
import psutil
import gc
from unittest.mock import Mock, patch
from datetime import datetime
import sys
import os

# Add necessary paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utility')))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, when, lit
import random


class TestDQXPerformance:
    """Performance tests for DQX framework."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create optimized Spark session for performance testing."""
        spark = SparkSession.builder \
            .appName("DQXPerformanceTest") \
            .config("spark.master", "local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture(params=[1000, 10000, 100000])
    def sample_data_sizes(self, request, spark_session):
        """Create sample data of different sizes for performance testing."""
        size = request.param
        data = []

        for i in range(size):
            data.append((
                f"ORD{i:08d}",
                f"ITEM{i % 1000:05d}",
                random.randint(1, 100),
                round(random.uniform(10.0, 1000.0), 2),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                f"STORE{i % 50:03d}",
                random.choice(["NEW", "PROCESSING", "SHIPPED", "DELIVERED"])
            ))

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("status", StringType(), True)
        ])

        df = spark_session.createDataFrame(data, schema)
        return df, size

    def test_dqx_rule_execution_performance(self, sample_data_sizes):
        """Test performance of DQX rule execution across different data sizes."""
        df, size = sample_data_sizes

        # Define complex DQX rules
        dqx_rules = [
            {"name": "completeness", "columns": ["order_id", "item_id"]},
            {"name": "validity", "column": "quantity", "condition": "quantity > 0"},
            {"name": "range_check", "column": "amount", "min": 0, "max": 10000},
            {"name": "format", "column": "timestamp", "pattern": r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"},
            {"name": "enum_check", "column": "status", "values": ["NEW", "PROCESSING", "SHIPPED", "DELIVERED"]}
        ]

        start_time = time.time()
        memory_before = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        # Apply all DQX rules
        result_df = df

        # Completeness check
        result_df = result_df.withColumn(
            "dqx_completeness",
            when(col("order_id").isNotNull() & col("item_id").isNotNull(), 1).otherwise(0)
        )

        # Validity check
        result_df = result_df.withColumn(
            "dqx_validity",
            when(col("quantity") > 0, 1).otherwise(0)
        )

        # Range check
        result_df = result_df.withColumn(
            "dqx_range",
            when((col("amount") >= 0) & (col("amount") <= 10000), 1).otherwise(0)
        )

        # Format check
        result_df = result_df.withColumn(
            "dqx_format",
            when(col("timestamp").rlike(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), 1).otherwise(0)
        )

        # Enum check
        result_df = result_df.withColumn(
            "dqx_enum",
            when(col("status").isin(["NEW", "PROCESSING", "SHIPPED", "DELIVERED"]), 1).otherwise(0)
        )

        # Calculate overall DQX score
        result_df = result_df.withColumn(
            "dqx_score",
            (col("dqx_completeness") + col("dqx_validity") + col("dqx_range") +
             col("dqx_format") + col("dqx_enum")) / 5
        )

        # Force execution
        record_count = result_df.count()

        end_time = time.time()
        memory_after = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        execution_time = end_time - start_time
        memory_used = memory_after - memory_before
        throughput = size / execution_time

        # Performance assertions
        assert record_count == size
        assert execution_time < size * 0.001  # Should process 1000+ records per second
        assert memory_used < 500  # Should use less than 500MB additional memory
        assert throughput > 1000  # Should process > 1000 records/second

        print(f"Size: {size}, Time: {execution_time:.2f}s, Throughput: {throughput:.0f} rec/s, Memory: {memory_used:.1f}MB")

    def test_dqx_memory_usage_scaling(self, spark_session):
        """Test memory usage scaling with data size."""
        memory_measurements = []

        for size in [1000, 5000, 10000, 20000]:
            gc.collect()  # Clean up memory
            memory_before = psutil.Process().memory_info().rss / 1024 / 1024

            # Create data
            data = [(f"ORD{i}", f"ITEM{i}", i) for i in range(size)]
            df = spark_session.createDataFrame(data, ["order_id", "item_id", "quantity"])

            # Apply DQX validation
            validated_df = df.withColumn(
                "dqx_valid",
                when(col("quantity") > 0, True).otherwise(False)
            )

            # Force materialization
            validated_df.cache()
            validated_df.count()

            memory_after = psutil.Process().memory_info().rss / 1024 / 1024
            memory_used = memory_after - memory_before

            memory_measurements.append((size, memory_used))

            # Cleanup
            validated_df.unpersist()

        # Check memory scaling is reasonable (should be roughly linear)
        memory_per_1k_records = [mem / (size / 1000) for size, mem in memory_measurements]

        # Memory usage per 1K records should be consistent (within 50% variance)
        avg_memory_per_1k = sum(memory_per_1k_records) / len(memory_per_1k_records)
        for memory_1k in memory_per_1k_records:
            assert abs(memory_1k - avg_memory_per_1k) / avg_memory_per_1k < 0.5

        print(f"Memory scaling: {memory_measurements}")

    def test_dqx_concurrent_processing(self, spark_session):
        """Test DQX performance with concurrent processing."""
        import threading
        import concurrent.futures

        def process_partition(partition_id, records_per_partition=10000):
            """Process a data partition with DQX rules."""
            data = [
                (f"ORD{i}_{partition_id}", f"ITEM{i}", i % 100)
                for i in range(records_per_partition)
            ]

            df = spark_session.createDataFrame(data, ["order_id", "item_id", "quantity"])

            # Apply DQX validation
            validated = df.withColumn(
                "dqx_valid",
                when(col("quantity") > 0, True).otherwise(False)
            )

            return validated.count()

        start_time = time.time()

        # Process multiple partitions concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_partition, i)
                for i in range(4)
            ]

            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        end_time = time.time()
        total_time = end_time - start_time
        total_records = sum(results)
        throughput = total_records / total_time

        assert len(results) == 4
        assert all(result == 10000 for result in results)
        assert throughput > 5000  # Should achieve high throughput with concurrency
        assert total_time < 10  # Should complete within reasonable time

        print(f"Concurrent processing: {total_records} records in {total_time:.2f}s, Throughput: {throughput:.0f} rec/s")

    def test_dqx_complex_rule_performance(self, spark_session):
        """Test performance of complex DQX rules."""
        size = 50000

        # Create complex data
        data = []
        for i in range(size):
            data.append((
                f"ORD{i:08d}",
                f"ITEM{i % 1000:05d}",
                random.randint(1, 100),
                round(random.uniform(10.0, 1000.0), 2),
                f"EMAIL{i}@example.com",
                f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                random.choice(["CREDIT", "DEBIT", "CASH", "CHECK"])
            ))

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("payment_method", StringType(), True)
        ])

        df = spark_session.createDataFrame(data, schema)

        start_time = time.time()

        # Apply complex DQX rules
        validated_df = df.withColumn(
            "dqx_email_valid",
            when(col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"), 1).otherwise(0)
        ).withColumn(
            "dqx_phone_valid",
            when(col("phone").rlike(r"^\d{3}-\d{3}-\d{4}$"), 1).otherwise(0)
        ).withColumn(
            "dqx_payment_valid",
            when(col("payment_method").isin(["CREDIT", "DEBIT", "CASH", "CHECK"]), 1).otherwise(0)
        ).withColumn(
            "dqx_amount_range",
            when((col("amount") >= 10) & (col("amount") <= 1000), 1).otherwise(0)
        ).withColumn(
            "dqx_quantity_positive",
            when(col("quantity") > 0, 1).otherwise(0)
        )

        # Calculate composite score
        validated_df = validated_df.withColumn(
            "dqx_composite_score",
            (col("dqx_email_valid") + col("dqx_phone_valid") + col("dqx_payment_valid") +
             col("dqx_amount_range") + col("dqx_quantity_positive")) / 5
        )

        # Force execution and collect statistics
        record_count = validated_df.count()
        high_quality_count = validated_df.filter(col("dqx_composite_score") >= 0.8).count()

        end_time = time.time()
        execution_time = end_time - start_time
        throughput = size / execution_time

        # Performance assertions for complex rules
        assert record_count == size
        assert execution_time < 30  # Should complete complex validation within 30 seconds
        assert throughput > 1000  # Should maintain throughput > 1000 records/second
        assert high_quality_count > 0  # Should identify some high-quality records

        print(f"Complex rules: {size} records in {execution_time:.2f}s, Throughput: {throughput:.0f} rec/s")

    @pytest.mark.benchmark
    def test_dqx_benchmark_comparison(self, spark_session):
        """Benchmark DQX performance against baseline."""
        size = 100000

        # Create benchmark data
        data = [(f"ORD{i}", f"ITEM{i}", i) for i in range(size)]
        df = spark_session.createDataFrame(data, ["order_id", "item_id", "quantity"])

        # Baseline: Simple filter operation
        start_baseline = time.time()
        baseline_result = df.filter(col("quantity") > 0).count()
        baseline_time = time.time() - start_baseline

        # DQX: Validation with scoring
        start_dqx = time.time()
        dqx_result = df.withColumn(
            "dqx_score",
            when(col("quantity") > 0, 1.0).otherwise(0.0)
        ).count()
        dqx_time = time.time() - start_dqx

        # Performance comparison
        overhead_ratio = dqx_time / baseline_time

        assert baseline_result == size  # All records should pass simple filter
        assert dqx_result == size  # All records should be processed
        assert overhead_ratio < 2.0  # DQX should not be more than 2x slower than baseline
        assert dqx_time < 10  # Should complete within 10 seconds

        print(f"Baseline: {baseline_time:.2f}s, DQX: {dqx_time:.2f}s, Overhead: {overhead_ratio:.2f}x")

    def test_dqx_resource_utilization(self, spark_session):
        """Monitor resource utilization during DQX processing."""
        import threading

        # Resource monitoring
        cpu_usage = []
        memory_usage = []
        monitoring_active = True

        def monitor_resources():
            while monitoring_active:
                cpu_usage.append(psutil.cpu_percent(interval=0.1))
                memory_usage.append(psutil.Process().memory_percent())
                time.sleep(0.5)

        # Start monitoring
        monitor_thread = threading.Thread(target=monitor_resources)
        monitor_thread.start()

        try:
            # Create large dataset
            size = 200000
            data = [(f"ORD{i}", i, i * 1.5) for i in range(size)]
            df = spark_session.createDataFrame(data, ["order_id", "quantity", "amount"])

            # Apply resource-intensive DQX operations
            result_df = df.withColumn(
                "dqx_complex",
                when(
                    (col("quantity") > 0) &
                    (col("amount") > 0) &
                    (col("order_id").isNotNull()),
                    1.0
                ).otherwise(0.0)
            )

            # Force computation
            result_df.cache()
            final_count = result_df.count()

        finally:
            monitoring_active = False
            monitor_thread.join()

        # Analyze resource usage
        avg_cpu = sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0
        max_cpu = max(cpu_usage) if cpu_usage else 0
        avg_memory = sum(memory_usage) / len(memory_usage) if memory_usage else 0
        max_memory = max(memory_usage) if memory_usage else 0

        # Resource utilization assertions
        assert final_count == size
        assert avg_cpu < 80  # Average CPU should not exceed 80%
        assert max_cpu < 95  # Peak CPU should not exceed 95%
        assert avg_memory < 70  # Average memory should not exceed 70%
        assert max_memory < 85  # Peak memory should not exceed 85%

        print(f"Resource usage - CPU: avg={avg_cpu:.1f}%, max={max_cpu:.1f}%, Memory: avg={avg_memory:.1f}%, max={max_memory:.1f}%")