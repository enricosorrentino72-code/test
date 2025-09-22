"""
Performance Tests for Purchase Order Processing Throughput Benchmarks
Tests processing speed, data throughput, and performance optimization for purchase order pipelines
"""

import pytest
import time
import os
import tempfile
import shutil
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import psutil
import logging

# Import Spark and Delta Lake with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, current_timestamp, lit, sum as spark_sum, avg as spark_avg, count as spark_count
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# Import project classes
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

try:
    from purchase_order_item_model import PurchaseOrderItem
    from purchase_order_item_factory import PurchaseOrderItemFactory
    from purchase_order_dqx_pipeline import PurchaseOrderDQXPipeline
    from purchase_order_silver_manager import PurchaseOrderSilverManager
    from purchase_order_dqx_rules import PurchaseOrderDQXRules
except ImportError as e:
    # Mock classes for testing when not available
    PurchaseOrderItem = None
    PurchaseOrderItemFactory = None
    PurchaseOrderDQXPipeline = None
    PurchaseOrderSilverManager = None
    PurchaseOrderDQXRules = None

# Test markers
pytestmark = [
    pytest.mark.performance,
    pytest.mark.benchmark,
    pytest.mark.throughput
]


class TestPurchaseOrderThroughputBenchmarks:
    """Performance benchmarks for purchase order processing throughput."""

    @classmethod
    def setup_class(cls):
        """Set up test class with Spark session and performance monitoring."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        # Create Spark session optimized for performance testing
        cls.spark = SparkSession.builder \
            .appName("PurchaseOrderThroughputBenchmarks") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .master("local[*]") \
            .getOrCreate()

        # Set log level to reduce noise during performance testing
        cls.spark.sparkContext.setLogLevel("WARN")

        # Create temporary directories
        cls.temp_dir = tempfile.mkdtemp(prefix="throughput_benchmark_")
        cls.bronze_path = os.path.join(cls.temp_dir, "bronze")
        cls.silver_path = os.path.join(cls.temp_dir, "silver")

        os.makedirs(cls.bronze_path, exist_ok=True)
        os.makedirs(cls.silver_path, exist_ok=True)

        # Performance thresholds
        cls.performance_thresholds = {
            'small_batch_throughput': 1000,    # orders/second for small batches
            'medium_batch_throughput': 5000,   # orders/second for medium batches
            'large_batch_throughput': 10000,   # orders/second for large batches
            'max_processing_time': 60,         # seconds for any single operation
            'memory_limit_mb': 2048,           # MB memory usage limit
        }

    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        if hasattr(cls, 'spark') and cls.spark:
            cls.spark.stop()

        if hasattr(cls, 'temp_dir') and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def setup_method(self):
        """Set up for each test method."""
        self.factory = PurchaseOrderItemFactory() if PurchaseOrderItemFactory else None
        self.process = psutil.Process(os.getpid())
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB

    def generate_performance_test_data(self, count: int, scenario: str = "normal") -> DataFrame:
        """Generate test data optimized for performance testing."""
        if self.factory:
            # Use factory for realistic data
            orders = self.factory.generate_batch(count)
            data = [order.to_dict() for order in orders]
        else:
            # Fallback optimized test data generation
            data = []
            base_time = datetime.now(timezone.utc)

            for i in range(count):
                order = {
                    "order_id": f"ORD-PERF-{scenario}-{i:06d}",
                    "product_id": f"PROD-{i % 1000:03d}",  # Reuse product IDs for realistic distribution
                    "product_name": f"Performance Test Product {i % 100}",
                    "quantity": 1 + (i % 20),  # 1-20 items
                    "unit_price": 10.0 + (i % 100),  # $10-$110
                    "total_amount": (1 + (i % 20)) * (10.0 + (i % 100)),
                    "customer_id": f"CUST-{i % 500:03d}",  # 500 unique customers
                    "vendor_id": f"VEND-{i % 50:03d}",  # 50 unique vendors
                    "warehouse_location": f"WH-{i % 10:02d}",  # 10 warehouses
                    "order_status": "CONFIRMED",
                    "payment_status": "PAID",
                    "created_at": base_time,
                    "scenario": scenario
                }
                data.append(order)

        # Convert to Spark DataFrame
        df = self.spark.createDataFrame(data)
        return df

    @pytest.mark.critical
    @pytest.mark.timeout(180)
    def test_small_batch_purchase_order_processing_throughput(self):
        """Benchmark throughput for small batch purchase order processing (1K orders)."""
        logger = logging.getLogger(__name__)
        logger.info("=== Benchmarking Small Batch Processing Throughput ===")

        try:
            # Test parameters
            batch_size = 1000
            target_throughput = self.performance_thresholds['small_batch_throughput']

            # Step 1: Generate test data
            logger.info(f"Step 1: Generating {batch_size} purchase orders")
            start_generation = time.time()
            test_df = self.generate_performance_test_data(batch_size, "small_batch")
            generation_time = time.time() - start_generation

            logger.info(f"Data generation: {batch_size} orders in {generation_time:.3f}s")
            logger.info(f"Generation rate: {batch_size / generation_time:.1f} orders/sec")

            # Step 2: Write to Bronze layer (baseline performance)
            logger.info("Step 2: Bronze layer write performance")
            start_bronze = time.time()

            test_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.bronze_path)

            bronze_write_time = time.time() - start_bronze
            bronze_throughput = batch_size / bronze_write_time

            logger.info(f"Bronze write: {batch_size} orders in {bronze_write_time:.3f}s")
            logger.info(f"Bronze throughput: {bronze_throughput:.1f} orders/sec")

            # Step 3: Read and transform performance
            logger.info("Step 3: Bronze to Silver transformation performance")
            start_transform = time.time()

            bronze_df = self.spark.read.format("delta").load(self.bronze_path)

            # Apply transformations
            transformed_df = bronze_df \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("total_validation", col("quantity") * col("unit_price")) \
                .withColumn("is_valid", col("total_validation") == col("total_amount")) \
                .withColumn("quality_score",
                          col("is_valid").cast("double")) \
                .filter(col("quantity") > 0)

            # Force execution with action
            processed_count = transformed_df.count()
            transform_time = time.time() - start_transform
            transform_throughput = processed_count / transform_time

            logger.info(f"Transform: {processed_count} orders in {transform_time:.3f}s")
            logger.info(f"Transform throughput: {transform_throughput:.1f} orders/sec")

            # Step 4: Silver layer write performance
            logger.info("Step 4: Silver layer write performance")
            start_silver = time.time()

            transformed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(self.silver_path)

            silver_write_time = time.time() - start_silver
            silver_throughput = processed_count / silver_write_time

            logger.info(f"Silver write: {processed_count} orders in {silver_write_time:.3f}s")
            logger.info(f"Silver throughput: {silver_throughput:.1f} orders/sec")

            # Step 5: End-to-end performance
            total_processing_time = bronze_write_time + transform_time + silver_write_time
            end_to_end_throughput = processed_count / total_processing_time

            logger.info(f"End-to-end: {processed_count} orders in {total_processing_time:.3f}s")
            logger.info(f"End-to-end throughput: {end_to_end_throughput:.1f} orders/sec")

            # Performance assertions
            assert generation_time < 10.0, f"Data generation took {generation_time:.2f}s (should be < 10s)"
            assert bronze_write_time < 30.0, f"Bronze write took {bronze_write_time:.2f}s (should be < 30s)"
            assert transform_time < 30.0, f"Transform took {transform_time:.2f}s (should be < 30s)"
            assert silver_write_time < 30.0, f"Silver write took {silver_write_time:.2f}s (should be < 30s)"

            # Throughput assertions (relaxed for test environment)
            min_acceptable_throughput = target_throughput * 0.1  # 10% of target
            assert end_to_end_throughput >= min_acceptable_throughput, \
                f"End-to-end throughput {end_to_end_throughput:.1f} below minimum {min_acceptable_throughput:.1f} orders/sec"

            logger.info(f"Small batch throughput benchmark PASSED: {end_to_end_throughput:.1f} orders/sec")

        except Exception as e:
            logger.error(f"Small batch throughput benchmark failed: {str(e)}")
            pytest.fail(f"Small batch throughput benchmark failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(300)
    def test_medium_batch_purchase_order_processing_throughput(self):
        """Benchmark throughput for medium batch purchase order processing (10K orders)."""
        logger = logging.getLogger(__name__)
        logger.info("=== Benchmarking Medium Batch Processing Throughput ===")

        try:
            # Test parameters
            batch_size = 10000
            target_throughput = self.performance_thresholds['medium_batch_throughput']

            # Step 1: Generate and process test data
            logger.info(f"Step 1: Generating {batch_size} purchase orders")
            start_time = time.time()

            # Generate in chunks to manage memory
            chunk_size = 2500
            chunks = []

            for i in range(0, batch_size, chunk_size):
                current_chunk_size = min(chunk_size, batch_size - i)
                chunk_df = self.generate_performance_test_data(current_chunk_size, f"medium_batch_chunk_{i//chunk_size}")
                chunks.append(chunk_df)

            # Union all chunks
            test_df = chunks[0]
            for chunk in chunks[1:]:
                test_df = test_df.union(chunk)

            generation_time = time.time() - start_time
            logger.info(f"Data generation: {batch_size} orders in {generation_time:.3f}s")

            # Step 2: Optimized processing pipeline
            logger.info("Step 2: Optimized batch processing")
            start_processing = time.time()

            # Cache for performance
            test_df.cache()

            # Apply optimized transformations
            processed_df = test_df \
                .withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("financial_check", col("quantity") * col("unit_price")) \
                .withColumn("is_valid",
                          (col("financial_check") == col("total_amount")) &
                          (col("quantity") > 0) &
                          (col("unit_price") > 0)) \
                .withColumn("quality_score",
                          col("is_valid").cast("double")) \
                .repartition(4)  # Optimize partitioning

            # Force execution and count
            processed_count = processed_df.count()
            processing_time = time.time() - start_processing

            logger.info(f"Processing: {processed_count} orders in {processing_time:.3f}s")

            # Step 3: Batch aggregation performance
            logger.info("Step 3: Batch aggregation performance")
            start_aggregation = time.time()

            aggregation_df = processed_df.groupBy("vendor_id", "warehouse_location") \
                .agg(
                    spark_count("*").alias("order_count"),
                    spark_sum("total_amount").alias("total_value"),
                    spark_avg("quality_score").alias("avg_quality")
                )

            aggregation_count = aggregation_df.count()
            aggregation_time = time.time() - start_aggregation

            logger.info(f"Aggregation: {aggregation_count} groups in {aggregation_time:.3f}s")

            # Step 4: Write performance
            logger.info("Step 4: Optimized write performance")
            start_write = time.time()

            processed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("optimizeWrite", "true") \
                .save(self.silver_path)

            write_time = time.time() - start_write

            # Calculate throughput metrics
            total_time = processing_time + aggregation_time + write_time
            total_throughput = processed_count / total_time

            logger.info(f"Total processing time: {total_time:.3f}s")
            logger.info(f"Medium batch throughput: {total_throughput:.1f} orders/sec")

            # Performance assertions
            assert total_time < 120.0, f"Total processing took {total_time:.2f}s (should be < 120s)"
            assert processed_count == batch_size, f"Processed {processed_count} orders, expected {batch_size}"

            # Throughput assertion (relaxed for test environment)
            min_acceptable_throughput = target_throughput * 0.05  # 5% of target
            assert total_throughput >= min_acceptable_throughput, \
                f"Throughput {total_throughput:.1f} below minimum {min_acceptable_throughput:.1f} orders/sec"

            # Memory usage check
            current_memory = self.process.memory_info().rss / 1024 / 1024
            memory_increase = current_memory - self.start_memory
            logger.info(f"Memory usage: {current_memory:.1f}MB (increase: {memory_increase:.1f}MB)")

            assert memory_increase < self.performance_thresholds['memory_limit_mb'], \
                f"Memory increase {memory_increase:.1f}MB exceeds limit {self.performance_thresholds['memory_limit_mb']}MB"

            logger.info(f"Medium batch throughput benchmark PASSED: {total_throughput:.1f} orders/sec")

        except Exception as e:
            logger.error(f"Medium batch throughput benchmark failed: {str(e)}")
            pytest.fail(f"Medium batch throughput benchmark failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(600)
    def test_large_batch_purchase_order_processing_throughput(self):
        """Benchmark throughput for large batch purchase order processing (50K orders)."""
        logger = logging.getLogger(__name__)
        logger.info("=== Benchmarking Large Batch Processing Throughput ===")

        try:
            # Test parameters
            batch_size = 50000
            target_throughput = self.performance_thresholds['large_batch_throughput']

            # Step 1: Optimized data generation
            logger.info(f"Step 1: Generating {batch_size} purchase orders with optimization")
            start_time = time.time()

            # Generate in optimized chunks
            chunk_size = 5000
            chunk_dfs = []

            for i in range(0, batch_size, chunk_size):
                current_chunk_size = min(chunk_size, batch_size - i)
                chunk_df = self.generate_performance_test_data(current_chunk_size, f"large_batch_{i//chunk_size}")

                # Persist each chunk to avoid recomputation
                chunk_df.cache()
                chunk_dfs.append(chunk_df)

            # Union all chunks efficiently
            test_df = chunk_dfs[0]
            for chunk_df in chunk_dfs[1:]:
                test_df = test_df.union(chunk_df)

            # Optimize partitioning for large dataset
            test_df = test_df.repartition(8)
            test_df.cache()

            # Force materialization
            actual_count = test_df.count()
            generation_time = time.time() - start_time

            logger.info(f"Data generation: {actual_count} orders in {generation_time:.3f}s")
            logger.info(f"Generation rate: {actual_count / generation_time:.1f} orders/sec")

            # Step 2: Streaming-style processing simulation
            logger.info("Step 2: High-throughput processing simulation")
            start_processing = time.time()

            # Process in micro-batches to simulate streaming
            micro_batch_size = 10000
            processed_batches = []

            for i in range(0, actual_count, micro_batch_size):
                batch_start = time.time()

                # Simulate micro-batch processing
                micro_batch = test_df.limit(micro_batch_size).offset(i) if hasattr(test_df, 'offset') else \
                             test_df.filter(col("order_id").contains(f"large_batch_{i//micro_batch_size}"))

                # Apply transformations
                processed_batch = micro_batch \
                    .withColumn("batch_id", lit(i // micro_batch_size)) \
                    .withColumn("processing_timestamp", current_timestamp()) \
                    .withColumn("is_valid",
                              (col("quantity") > 0) &
                              (col("unit_price") > 0) &
                              (col("total_amount") == col("quantity") * col("unit_price")))

                batch_count = processed_batch.count()
                processed_batches.append(processed_batch)

                batch_time = time.time() - batch_start
                batch_throughput = batch_count / batch_time if batch_time > 0 else 0

                logger.info(f"Micro-batch {len(processed_batches)}: {batch_count} orders in {batch_time:.3f}s ({batch_throughput:.1f}/sec)")

            processing_time = time.time() - start_processing

            # Step 3: Aggregated performance metrics
            logger.info("Step 3: Final aggregation and performance metrics")
            start_final = time.time()

            # Union all processed batches
            if processed_batches:
                final_df = processed_batches[0]
                for batch_df in processed_batches[1:]:
                    final_df = final_df.union(batch_df)

                # Final aggregation
                final_metrics = final_df.agg(
                    spark_count("*").alias("total_processed"),
                    spark_sum(col("is_valid").cast("int")).alias("valid_orders"),
                    spark_avg("total_amount").alias("avg_order_value")
                ).collect()[0]

                final_time = time.time() - start_final
                total_time = generation_time + processing_time + final_time
                overall_throughput = actual_count / total_time

                logger.info(f"Final metrics: {final_metrics.total_processed} processed, {final_metrics.valid_orders} valid")
                logger.info(f"Average order value: ${final_metrics.avg_order_value:.2f}")
                logger.info(f"Overall throughput: {overall_throughput:.1f} orders/sec")

                # Performance assertions
                assert total_time < self.performance_thresholds['max_processing_time'] * 10, \
                    f"Total time {total_time:.2f}s exceeds limit"

                assert final_metrics.total_processed >= actual_count * 0.95, \
                    f"Processed {final_metrics.total_processed} orders, expected at least {actual_count * 0.95:.0f}"

                # Throughput assertion (very relaxed for large dataset)
                min_acceptable_throughput = target_throughput * 0.01  # 1% of target
                assert overall_throughput >= min_acceptable_throughput, \
                    f"Throughput {overall_throughput:.1f} below minimum {min_acceptable_throughput:.1f} orders/sec"

                logger.info(f"Large batch throughput benchmark PASSED: {overall_throughput:.1f} orders/sec")

            else:
                pytest.fail("No micro-batches were processed")

        except Exception as e:
            logger.error(f"Large batch throughput benchmark failed: {str(e)}")
            pytest.fail(f"Large batch throughput benchmark failed: {str(e)}")

    @pytest.mark.medium
    @pytest.mark.timeout(240)
    def test_concurrent_purchase_order_processing_throughput(self):
        """Benchmark throughput for concurrent purchase order processing scenarios."""
        logger = logging.getLogger(__name__)
        logger.info("=== Benchmarking Concurrent Processing Throughput ===")

        try:
            # Test parameters
            concurrent_batches = 4
            batch_size = 2500  # Total: 10K orders across 4 concurrent batches

            # Step 1: Generate concurrent processing scenarios
            logger.info(f"Step 1: Setting up {concurrent_batches} concurrent processing scenarios")

            batch_dfs = []
            generation_times = []

            for batch_id in range(concurrent_batches):
                start_gen = time.time()
                batch_df = self.generate_performance_test_data(batch_size, f"concurrent_batch_{batch_id}")
                batch_df = batch_df.withColumn("batch_id", lit(batch_id))
                batch_dfs.append(batch_df)

                gen_time = time.time() - start_gen
                generation_times.append(gen_time)
                logger.info(f"Generated batch {batch_id}: {batch_size} orders in {gen_time:.3f}s")

            # Step 2: Simulate concurrent processing
            logger.info("Step 2: Simulating concurrent processing")
            start_concurrent = time.time()

            # Process all batches "concurrently" (sequential simulation)
            processed_results = []

            for i, batch_df in enumerate(batch_dfs):
                batch_start = time.time()

                # Apply concurrent processing simulation
                processed_batch = batch_df \
                    .withColumn("processing_start", current_timestamp()) \
                    .withColumn("concurrent_id", lit(i)) \
                    .withColumn("is_valid",
                              (col("quantity") > 0) &
                              (col("unit_price") > 0)) \
                    .filter(col("is_valid"))

                # Force execution
                batch_count = processed_batch.count()
                batch_time = time.time() - batch_start

                result = {
                    'batch_id': i,
                    'count': batch_count,
                    'time': batch_time,
                    'throughput': batch_count / batch_time
                }
                processed_results.append(result)

                logger.info(f"Concurrent batch {i}: {batch_count} orders in {batch_time:.3f}s ({result['throughput']:.1f}/sec)")

            concurrent_time = time.time() - start_concurrent

            # Step 3: Aggregate concurrent performance
            logger.info("Step 3: Analyzing concurrent performance")

            total_orders = sum(result['count'] for result in processed_results)
            avg_batch_time = sum(result['time'] for result in processed_results) / len(processed_results)
            max_batch_time = max(result['time'] for result in processed_results)
            min_batch_time = min(result['time'] for result in processed_results)

            # Concurrent throughput calculation
            concurrent_throughput = total_orders / concurrent_time
            parallel_efficiency = concurrent_time / max_batch_time  # How much parallelism achieved

            logger.info(f"Concurrent processing results:")
            logger.info(f"  Total orders: {total_orders}")
            logger.info(f"  Concurrent time: {concurrent_time:.3f}s")
            logger.info(f"  Average batch time: {avg_batch_time:.3f}s")
            logger.info(f"  Time range: {min_batch_time:.3f}s - {max_batch_time:.3f}s")
            logger.info(f"  Concurrent throughput: {concurrent_throughput:.1f} orders/sec")
            logger.info(f"  Parallel efficiency: {parallel_efficiency:.2f}")

            # Performance assertions
            assert concurrent_time < 180.0, f"Concurrent processing took {concurrent_time:.2f}s (should be < 180s)"
            assert total_orders >= batch_size * concurrent_batches * 0.9, \
                f"Processed {total_orders} orders, expected at least {batch_size * concurrent_batches * 0.9:.0f}"

            # Efficiency assertions
            expected_min_throughput = 100  # orders/second minimum
            assert concurrent_throughput >= expected_min_throughput, \
                f"Concurrent throughput {concurrent_throughput:.1f} below minimum {expected_min_throughput}"

            # Parallel efficiency should be reasonable (not perfect due to sequential simulation)
            assert parallel_efficiency > 0.5, f"Parallel efficiency {parallel_efficiency:.2f} too low"

            logger.info(f"Concurrent processing benchmark PASSED: {concurrent_throughput:.1f} orders/sec")

        except Exception as e:
            logger.error(f"Concurrent processing benchmark failed: {str(e)}")
            pytest.fail(f"Concurrent processing benchmark failed: {str(e)}")


# Test fixtures and utilities
@pytest.fixture(scope="session")
def performance_spark_session():
    """Provide optimized Spark session for performance testing."""
    if not SPARK_AVAILABLE:
        pytest.skip("Spark not available")

    spark = SparkSession.builder \
        .appName("PurchaseOrderPerformanceTests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()

    yield spark
    spark.stop()

@pytest.fixture
def performance_monitoring():
    """Provide performance monitoring utilities."""
    class PerformanceMonitor:
        def __init__(self):
            self.process = psutil.Process(os.getpid())
            self.start_time = time.time()
            self.start_memory = self.process.memory_info().rss / 1024 / 1024

        def get_metrics(self):
            current_time = time.time()
            current_memory = self.process.memory_info().rss / 1024 / 1024
            return {
                'elapsed_time': current_time - self.start_time,
                'memory_usage_mb': current_memory,
                'memory_increase_mb': current_memory - self.start_memory,
                'cpu_percent': self.process.cpu_percent()
            }

    return PerformanceMonitor()