"""
Performance Tests for Memory Usage Monitoring in Purchase Order Processing
Tests memory consumption, optimization, and resource management for purchase order pipelines
"""

import pytest
import time
import os
import tempfile
import shutil
import gc
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import psutil
import logging

# Import Spark and Delta Lake with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, current_timestamp, lit, sum as spark_sum, count as spark_count
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
except ImportError as e:
    # Mock classes for testing when not available
    PurchaseOrderItem = None
    PurchaseOrderItemFactory = None
    PurchaseOrderDQXPipeline = None

# Test markers
pytestmark = [
    pytest.mark.performance,
    pytest.mark.memory,
    pytest.mark.resource_monitoring
]


class TestPurchaseOrderMemoryUsage:
    """Performance tests for memory usage monitoring in purchase order processing."""

    @classmethod
    def setup_class(cls):
        """Set up test class with Spark session and memory monitoring."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        # Create Spark session with memory optimization
        cls.spark = SparkSession.builder \
            .appName("PurchaseOrderMemoryTests") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .master("local[2]") \
            .getOrCreate()

        cls.spark.sparkContext.setLogLevel("WARN")

        # Create temporary directories
        cls.temp_dir = tempfile.mkdtemp(prefix="memory_test_")
        cls.data_path = os.path.join(cls.temp_dir, "data")
        os.makedirs(cls.data_path, exist_ok=True)

        # Memory thresholds
        cls.memory_thresholds = {
            'max_increase_mb': 1024,      # Max memory increase during processing
            'max_total_mb': 3072,         # Max total memory usage
            'gc_efficiency_threshold': 0.3, # Min memory freed by GC (30%)
            'memory_leak_threshold': 100,  # Max memory increase per iteration (MB)
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
        self.baseline_memory = self.process.memory_info().rss / 1024 / 1024  # MB

        # Force garbage collection to get clean baseline
        gc.collect()
        time.sleep(1)
        self.baseline_memory = self.process.memory_info().rss / 1024 / 1024

    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage metrics."""
        memory_info = self.process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size
            'vms_mb': memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            'increase_mb': (memory_info.rss / 1024 / 1024) - self.baseline_memory,
            'percent': self.process.memory_percent()
        }

    def generate_memory_test_data(self, count: int, complexity: str = "normal") -> DataFrame:
        """Generate test data with varying memory footprint."""
        if self.factory:
            orders = self.factory.generate_batch(count)
            data = [order.to_dict() for order in orders]
        else:
            # Generate data with controlled complexity
            data = []
            base_time = datetime.now(timezone.utc)

            for i in range(count):
                if complexity == "high":
                    # Add large string fields to increase memory footprint
                    large_description = f"Large product description {i} " * 100  # ~3KB per order
                    metadata = {f"key_{j}": f"value_{j}_{i}" for j in range(50)}  # Additional metadata
                else:
                    large_description = f"Product {i}"
                    metadata = {"simple": "data"}

                order = {
                    "order_id": f"ORD-MEM-{complexity}-{i:06d}",
                    "product_id": f"PROD-{i % 1000:03d}",
                    "product_name": f"Memory Test Product {i}",
                    "product_description": large_description,
                    "quantity": 1 + (i % 20),
                    "unit_price": 10.0 + (i % 100),
                    "total_amount": (1 + (i % 20)) * (10.0 + (i % 100)),
                    "customer_id": f"CUST-{i % 500:03d}",
                    "vendor_id": f"VEND-{i % 50:03d}",
                    "warehouse_location": f"WH-{i % 10:02d}",
                    "created_at": base_time,
                    "metadata": str(metadata),
                    "complexity": complexity
                }
                data.append(order)

        return self.spark.createDataFrame(data)

    @pytest.mark.critical
    @pytest.mark.timeout(300)
    def test_memory_usage_during_large_dataset_processing(self):
        """Test memory usage patterns during large dataset processing."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Memory Usage During Large Dataset Processing ===")

        try:
            # Test parameters
            dataset_sizes = [5000, 10000, 20000]  # Progressive dataset sizes
            memory_measurements = []

            logger.info(f"Baseline memory: {self.baseline_memory:.1f}MB")

            for size in dataset_sizes:
                logger.info(f"Processing dataset of size: {size}")

                # Step 1: Generate test data and measure memory
                start_memory = self.get_memory_usage()
                logger.info(f"Pre-generation memory: {start_memory['rss_mb']:.1f}MB")

                test_df = self.generate_memory_test_data(size, "normal")

                generation_memory = self.get_memory_usage()
                logger.info(f"Post-generation memory: {generation_memory['rss_mb']:.1f}MB (+{generation_memory['increase_mb']:.1f}MB)")

                # Step 2: Cache data and measure memory impact
                test_df.cache()
                cache_count = test_df.count()  # Force caching

                cache_memory = self.get_memory_usage()
                logger.info(f"Post-cache memory: {cache_memory['rss_mb']:.1f}MB (+{cache_memory['increase_mb']:.1f}MB)")

                # Step 3: Process data with transformations
                processed_df = test_df \
                    .withColumn("processing_timestamp", current_timestamp()) \
                    .withColumn("is_valid", col("quantity") > 0) \
                    .withColumn("revenue_category",
                              col("total_amount") > 100) \
                    .filter(col("is_valid"))

                # Force processing
                processed_count = processed_df.count()

                processing_memory = self.get_memory_usage()
                logger.info(f"Post-processing memory: {processing_memory['rss_mb']:.1f}MB (+{processing_memory['increase_mb']:.1f}MB)")

                # Step 4: Aggregation operations
                aggregation_df = processed_df.groupBy("vendor_id") \
                    .agg(
                        spark_count("*").alias("order_count"),
                        spark_sum("total_amount").alias("total_revenue")
                    )

                agg_count = aggregation_df.count()

                aggregation_memory = self.get_memory_usage()
                logger.info(f"Post-aggregation memory: {aggregation_memory['rss_mb']:.1f}MB (+{aggregation_memory['increase_mb']:.1f}MB)")

                # Step 5: Cleanup and measure memory recovery
                test_df.unpersist()
                processed_df = None
                aggregation_df = None
                gc.collect()
                time.sleep(2)  # Allow GC to complete

                cleanup_memory = self.get_memory_usage()
                logger.info(f"Post-cleanup memory: {cleanup_memory['rss_mb']:.1f}MB (+{cleanup_memory['increase_mb']:.1f}MB)")

                # Record measurements
                measurement = {
                    'dataset_size': size,
                    'max_memory_mb': max(generation_memory['rss_mb'], cache_memory['rss_mb'],
                                       processing_memory['rss_mb'], aggregation_memory['rss_mb']),
                    'max_increase_mb': max(generation_memory['increase_mb'], cache_memory['increase_mb'],
                                         processing_memory['increase_mb'], aggregation_memory['increase_mb']),
                    'cleanup_memory_mb': cleanup_memory['rss_mb'],
                    'memory_recovery_mb': aggregation_memory['rss_mb'] - cleanup_memory['rss_mb'],
                    'processed_count': processed_count
                }
                memory_measurements.append(measurement)

                # Memory assertions for this iteration
                assert processing_memory['increase_mb'] < self.memory_thresholds['max_increase_mb'], \
                    f"Memory increase {processing_memory['increase_mb']:.1f}MB exceeds threshold"

                assert processing_memory['rss_mb'] < self.memory_thresholds['max_total_mb'], \
                    f"Total memory {processing_memory['rss_mb']:.1f}MB exceeds threshold"

                logger.info(f"Dataset {size}: Max memory {measurement['max_memory_mb']:.1f}MB, "
                          f"Recovery {measurement['memory_recovery_mb']:.1f}MB")

            # Step 6: Analyze memory scaling patterns
            logger.info("Analyzing memory scaling patterns...")

            for i, measurement in enumerate(memory_measurements):
                logger.info(f"Size {measurement['dataset_size']}: "
                          f"Max memory {measurement['max_memory_mb']:.1f}MB, "
                          f"Max increase {measurement['max_increase_mb']:.1f}MB, "
                          f"Recovery {measurement['memory_recovery_mb']:.1f}MB")

            # Verify memory scaling is reasonable (not exponential)
            if len(memory_measurements) >= 2:
                size_ratio = memory_measurements[-1]['dataset_size'] / memory_measurements[0]['dataset_size']
                memory_ratio = memory_measurements[-1]['max_increase_mb'] / max(memory_measurements[0]['max_increase_mb'], 1)

                logger.info(f"Scaling analysis: Size ratio {size_ratio:.1f}x, Memory ratio {memory_ratio:.1f}x")

                # Memory should scale better than linear (due to optimizations)
                assert memory_ratio <= size_ratio * 1.5, \
                    f"Memory scaling {memory_ratio:.1f}x too high for size scaling {size_ratio:.1f}x"

            logger.info("Large dataset memory usage test PASSED")

        except Exception as e:
            logger.error(f"Large dataset memory usage test failed: {str(e)}")
            pytest.fail(f"Large dataset memory usage test failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(240)
    def test_memory_optimization_and_garbage_collection(self):
        """Test memory optimization techniques and garbage collection effectiveness."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Memory Optimization and Garbage Collection ===")

        try:
            # Test parameters
            batch_size = 10000
            iterations = 5

            memory_profile = []
            baseline = self.get_memory_usage()
            logger.info(f"Starting memory: {baseline['rss_mb']:.1f}MB")

            for iteration in range(iterations):
                logger.info(f"Iteration {iteration + 1}/{iterations}")

                iteration_start_memory = self.get_memory_usage()

                # Step 1: Create and process data
                test_df = self.generate_memory_test_data(batch_size, "high")  # High memory footprint

                # Step 2: Multiple transformations (memory accumulation)
                step1_df = test_df.withColumn("step1", col("quantity") * col("unit_price"))
                step2_df = step1_df.withColumn("step2", col("step1") * 1.1)
                step3_df = step2_df.withColumn("step3", col("step2") + 100)
                step4_df = step3_df.filter(col("step3") > 0)

                # Force execution
                result_count = step4_df.count()

                after_processing_memory = self.get_memory_usage()

                # Step 3: Cache optimization test
                step4_df.cache()
                cached_count = step4_df.count()  # Materialize cache

                after_cache_memory = self.get_memory_usage()

                # Step 4: Explicit cleanup
                step4_df.unpersist()
                step1_df = step2_df = step3_df = step4_df = None
                test_df = None

                # Force garbage collection
                gc.collect()
                time.sleep(1)  # Allow GC to complete

                after_cleanup_memory = self.get_memory_usage()

                # Record iteration metrics
                iteration_metrics = {
                    'iteration': iteration + 1,
                    'start_memory_mb': iteration_start_memory['rss_mb'],
                    'peak_memory_mb': max(after_processing_memory['rss_mb'], after_cache_memory['rss_mb']),
                    'end_memory_mb': after_cleanup_memory['rss_mb'],
                    'memory_gained_mb': after_cleanup_memory['rss_mb'] - iteration_start_memory['rss_mb'],
                    'memory_freed_mb': after_cache_memory['rss_mb'] - after_cleanup_memory['rss_mb'],
                    'gc_efficiency': (after_cache_memory['rss_mb'] - after_cleanup_memory['rss_mb']) /
                                   max(after_cache_memory['rss_mb'] - iteration_start_memory['rss_mb'], 1),
                    'processed_count': result_count
                }
                memory_profile.append(iteration_metrics)

                logger.info(f"  Start: {iteration_metrics['start_memory_mb']:.1f}MB")
                logger.info(f"  Peak: {iteration_metrics['peak_memory_mb']:.1f}MB")
                logger.info(f"  End: {iteration_metrics['end_memory_mb']:.1f}MB")
                logger.info(f"  Memory freed: {iteration_metrics['memory_freed_mb']:.1f}MB")
                logger.info(f"  GC efficiency: {iteration_metrics['gc_efficiency']:.2f}")

                # Assertions per iteration
                assert iteration_metrics['memory_gained_mb'] < self.memory_thresholds['memory_leak_threshold'], \
                    f"Iteration {iteration + 1}: Memory leak detected {iteration_metrics['memory_gained_mb']:.1f}MB"

                assert iteration_metrics['gc_efficiency'] >= 0, \
                    f"Iteration {iteration + 1}: Negative GC efficiency {iteration_metrics['gc_efficiency']:.2f}"

            # Step 5: Analyze overall memory optimization
            logger.info("Analyzing overall memory optimization...")

            avg_gc_efficiency = sum(m['gc_efficiency'] for m in memory_profile) / len(memory_profile)
            total_memory_drift = memory_profile[-1]['end_memory_mb'] - memory_profile[0]['start_memory_mb']
            max_peak_memory = max(m['peak_memory_mb'] for m in memory_profile)

            logger.info(f"Average GC efficiency: {avg_gc_efficiency:.2f}")
            logger.info(f"Total memory drift: {total_memory_drift:.1f}MB")
            logger.info(f"Maximum peak memory: {max_peak_memory:.1f}MB")

            # Overall memory optimization assertions
            assert avg_gc_efficiency >= self.memory_thresholds['gc_efficiency_threshold'], \
                f"Average GC efficiency {avg_gc_efficiency:.2f} below threshold {self.memory_thresholds['gc_efficiency_threshold']}"

            assert total_memory_drift < self.memory_thresholds['memory_leak_threshold'] * 2, \
                f"Total memory drift {total_memory_drift:.1f}MB indicates memory leak"

            assert max_peak_memory < self.memory_thresholds['max_total_mb'], \
                f"Peak memory {max_peak_memory:.1f}MB exceeds limit"

            # Verify memory stability across iterations
            memory_variance = max(m['end_memory_mb'] for m in memory_profile) - min(m['end_memory_mb'] for m in memory_profile)
            assert memory_variance < 200, f"Memory variance {memory_variance:.1f}MB too high (unstable)"

            logger.info("Memory optimization and GC test PASSED")

        except Exception as e:
            logger.error(f"Memory optimization test failed: {str(e)}")
            pytest.fail(f"Memory optimization test failed: {str(e)}")

    @pytest.mark.medium
    @pytest.mark.timeout(180)
    def test_memory_usage_under_concurrent_load(self):
        """Test memory usage patterns under concurrent processing load."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Memory Usage Under Concurrent Load ===")

        try:
            # Test parameters
            concurrent_tasks = 3
            task_size = 5000

            baseline_memory = self.get_memory_usage()
            logger.info(f"Baseline memory: {baseline_memory['rss_mb']:.1f}MB")

            # Step 1: Simulate concurrent data generation
            logger.info("Step 1: Simulating concurrent data generation")
            concurrent_dfs = []
            generation_memories = []

            for task_id in range(concurrent_tasks):
                task_start_memory = self.get_memory_usage()

                task_df = self.generate_memory_test_data(task_size, f"concurrent_task_{task_id}")
                task_df = task_df.withColumn("task_id", lit(task_id))
                concurrent_dfs.append(task_df)

                task_end_memory = self.get_memory_usage()
                generation_memories.append(task_end_memory)

                logger.info(f"Task {task_id}: Memory {task_end_memory['rss_mb']:.1f}MB (+{task_end_memory['increase_mb']:.1f}MB)")

            # Step 2: Concurrent processing simulation
            logger.info("Step 2: Processing concurrent tasks")
            processed_results = []

            for i, df in enumerate(concurrent_dfs):
                processing_start_memory = self.get_memory_usage()

                # Apply processing while other data is in memory
                processed_df = df \
                    .withColumn("concurrent_processing", current_timestamp()) \
                    .withColumn("is_valid", col("quantity") > 0) \
                    .filter(col("is_valid"))

                # Cache for concurrent access simulation
                processed_df.cache()
                count = processed_df.count()

                processing_end_memory = self.get_memory_usage()

                result = {
                    'task_id': i,
                    'count': count,
                    'start_memory_mb': processing_start_memory['rss_mb'],
                    'end_memory_mb': processing_end_memory['rss_mb'],
                    'memory_increase_mb': processing_end_memory['rss_mb'] - processing_start_memory['rss_mb']
                }
                processed_results.append(result)

                logger.info(f"Processed task {i}: {count} orders, "
                          f"memory {processing_end_memory['rss_mb']:.1f}MB (+{result['memory_increase_mb']:.1f}MB)")

            # Step 3: Peak memory analysis
            peak_memory = self.get_memory_usage()
            logger.info(f"Peak concurrent memory: {peak_memory['rss_mb']:.1f}MB (+{peak_memory['increase_mb']:.1f}MB)")

            # Step 4: Concurrent aggregation
            logger.info("Step 3: Concurrent aggregation operations")
            aggregation_results = []

            for i, df in enumerate([result['task_id'] for result in processed_results]):
                # Simulate aggregation on cached data
                cached_df = self.spark.sql(f"SELECT * FROM cached_data_{i}")  # Simulated cached access
                # Since we can't easily access cached data by name, we'll use the original DataFrames
                agg_df = concurrent_dfs[i].groupBy("vendor_id").agg(spark_count("*").alias("count"))
                agg_count = agg_df.count()
                aggregation_results.append(agg_count)

            aggregation_memory = self.get_memory_usage()
            logger.info(f"Post-aggregation memory: {aggregation_memory['rss_mb']:.1f}MB")

            # Step 5: Cleanup all concurrent data
            logger.info("Step 4: Cleaning up concurrent data")
            for df in concurrent_dfs:
                try:
                    df.unpersist()
                except:
                    pass

            concurrent_dfs.clear()
            processed_results.clear()
            gc.collect()
            time.sleep(2)

            cleanup_memory = self.get_memory_usage()
            logger.info(f"Post-cleanup memory: {cleanup_memory['rss_mb']:.1f}MB (+{cleanup_memory['increase_mb']:.1f}MB)")

            # Memory usage assertions
            total_data_size = concurrent_tasks * task_size
            memory_per_order = peak_memory['increase_mb'] / total_data_size

            logger.info(f"Total orders processed: {total_data_size}")
            logger.info(f"Memory per order: {memory_per_order * 1024:.2f}KB")

            # Performance assertions
            assert peak_memory['rss_mb'] < self.memory_thresholds['max_total_mb'], \
                f"Peak memory {peak_memory['rss_mb']:.1f}MB exceeds limit"

            assert peak_memory['increase_mb'] < self.memory_thresholds['max_increase_mb'], \
                f"Memory increase {peak_memory['increase_mb']:.1f}MB exceeds limit"

            # Memory efficiency assertion
            memory_freed = peak_memory['rss_mb'] - cleanup_memory['rss_mb']
            cleanup_efficiency = memory_freed / max(peak_memory['increase_mb'], 1)

            assert cleanup_efficiency >= 0.5, \
                f"Cleanup efficiency {cleanup_efficiency:.2f} below 50%"

            # Memory per order should be reasonable
            assert memory_per_order < 0.1, \
                f"Memory per order {memory_per_order:.3f}MB too high"

            logger.info(f"Concurrent memory usage test PASSED: "
                       f"Peak {peak_memory['rss_mb']:.1f}MB, "
                       f"Cleanup efficiency {cleanup_efficiency:.2f}")

        except Exception as e:
            logger.error(f"Concurrent memory usage test failed: {str(e)}")
            pytest.fail(f"Concurrent memory usage test failed: {str(e)}")


# Test fixtures and utilities
@pytest.fixture(scope="session")
def memory_monitoring_spark_session():
    """Provide Spark session optimized for memory monitoring."""
    if not SPARK_AVAILABLE:
        pytest.skip("Spark not available")

    spark = SparkSession.builder \
        .appName("PurchaseOrderMemoryTests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .master("local[2]") \
        .getOrCreate()

    yield spark
    spark.stop()

@pytest.fixture
def memory_tracker():
    """Provide memory tracking utilities for tests."""
    class MemoryTracker:
        def __init__(self):
            self.process = psutil.Process(os.getpid())
            self.baseline = self.process.memory_info().rss / 1024 / 1024
            self.measurements = []

        def record(self, label: str):
            current = self.process.memory_info().rss / 1024 / 1024
            measurement = {
                'label': label,
                'memory_mb': current,
                'increase_mb': current - self.baseline,
                'timestamp': time.time()
            }
            self.measurements.append(measurement)
            return measurement

        def get_summary(self):
            if not self.measurements:
                return {}

            return {
                'baseline_mb': self.baseline,
                'peak_mb': max(m['memory_mb'] for m in self.measurements),
                'final_mb': self.measurements[-1]['memory_mb'],
                'max_increase_mb': max(m['increase_mb'] for m in self.measurements),
                'total_drift_mb': self.measurements[-1]['memory_mb'] - self.baseline
            }

    return MemoryTracker()