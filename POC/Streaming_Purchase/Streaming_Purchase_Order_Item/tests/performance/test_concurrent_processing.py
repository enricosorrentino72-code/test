"""
Performance tests for concurrent processing operations.

Tests cover:
- Multi-threaded EventHub operations
- Parallel Spark processing
- Concurrent database operations
- Thread safety validation
- Resource contention analysis
"""

import pytest
import time
import threading
import concurrent.futures
from unittest.mock import Mock, patch
import sys
import os
import queue
import random

# Add necessary paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utility')))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime


class TestConcurrentProcessing:
    """Performance tests for concurrent processing."""

    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create Spark session optimized for concurrent processing."""
        spark = SparkSession.builder \
            .appName("ConcurrentProcessingTest") \
            .config("spark.master", "local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        yield spark
        spark.stop()

    def test_concurrent_eventhub_producers(self):
        """Test concurrent EventHub producer performance."""

        def mock_producer_worker(worker_id, message_count, results_queue):
            """Mock EventHub producer worker."""
            with patch('azure.eventhub.EventHubProducerClient') as mock_client:
                producer = Mock()
                mock_batch = Mock()
                producer.create_batch.return_value = mock_batch
                producer.send_batch = Mock()
                mock_client.from_connection_string.return_value = producer

                start_time = time.time()
                messages_sent = 0

                for i in range(message_count):
                    message = {
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now().isoformat(),
                        "data": f"test_data_{worker_id}_{i}"
                    }

                    # Simulate message creation and sending
                    mock_batch.add(message)
                    if (i + 1) % 100 == 0:  # Send in batches of 100
                        producer.send_batch(mock_batch)
                        messages_sent += 100

                end_time = time.time()
                execution_time = end_time - start_time

                results_queue.put({
                    "worker_id": worker_id,
                    "messages_sent": messages_sent,
                    "execution_time": execution_time,
                    "throughput": messages_sent / execution_time if execution_time > 0 else 0
                })

        # Test configuration
        num_workers = 4
        messages_per_worker = 1000
        results_queue = queue.Queue()

        # Start concurrent producers
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(mock_producer_worker, i, messages_per_worker, results_queue)
                for i in range(num_workers)
            ]

            # Wait for all workers to complete
            concurrent.futures.wait(futures)

        total_time = time.time() - start_time

        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())

        # Analyze performance
        total_messages = sum(r["messages_sent"] for r in results)
        avg_throughput = sum(r["throughput"] for r in results) / len(results)
        overall_throughput = total_messages / total_time

        # Performance assertions
        assert len(results) == num_workers
        assert total_messages == num_workers * messages_per_worker
        assert avg_throughput > 500  # Each worker should achieve > 500 msg/sec
        assert overall_throughput > 1000  # Overall throughput > 1000 msg/sec
        assert total_time < 10  # Should complete within 10 seconds

        print(f"Concurrent EventHub: {total_messages} messages, {total_time:.2f}s, {overall_throughput:.0f} msg/s")

    def test_parallel_spark_processing(self, spark_session):
        """Test parallel Spark processing performance."""

        def create_partition_data(partition_id, records_per_partition=10000):
            """Create data for a specific partition."""
            return [
                (f"ORD{i}_{partition_id}", f"ITEM{i}", random.randint(1, 100), random.uniform(10, 1000))
                for i in range(records_per_partition)
            ]

        # Configuration
        num_partitions = 8
        records_per_partition = 10000

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("amount", StringType(), True)
        ])

        # Create data for all partitions
        all_data = []
        for partition_id in range(num_partitions):
            partition_data = create_partition_data(partition_id, records_per_partition)
            all_data.extend(partition_data)

        df = spark_session.createDataFrame(all_data, schema)

        # Repartition for parallel processing
        df = df.repartition(num_partitions)

        start_time = time.time()

        # Parallel processing operations
        result_df = df.filter(df.quantity > 0) \
                     .groupBy("item_id") \
                     .agg({"quantity": "sum", "amount": "avg"}) \
                     .filter("sum(quantity) > 10")

        # Force execution
        result_count = result_df.count()

        end_time = time.time()
        execution_time = end_time - start_time

        total_records = num_partitions * records_per_partition
        throughput = total_records / execution_time

        # Performance assertions
        assert result_count > 0
        assert execution_time < 30  # Should complete within 30 seconds
        assert throughput > 10000  # Should process > 10K records/second

        print(f"Parallel Spark: {total_records} records in {execution_time:.2f}s, {throughput:.0f} rec/s")

    def test_concurrent_database_operations(self):
        """Test concurrent database operation performance."""

        def mock_database_worker(worker_id, operation_count, connection_pool, results):
            """Mock database worker performing CRUD operations."""
            worker_results = {
                "worker_id": worker_id,
                "operations_completed": 0,
                "execution_time": 0,
                "errors": 0
            }

            start_time = time.time()

            try:
                for i in range(operation_count):
                    # Simulate database operations
                    with connection_pool:  # Mock connection from pool
                        # Mock INSERT
                        time.sleep(0.001)  # Simulate DB latency

                        # Mock SELECT
                        time.sleep(0.0005)

                        # Mock UPDATE
                        time.sleep(0.001)

                        worker_results["operations_completed"] += 1

            except Exception as e:
                worker_results["errors"] += 1

            worker_results["execution_time"] = time.time() - start_time
            results.append(worker_results)

        # Mock connection pool
        class MockConnectionPool:
            def __init__(self, max_connections=10):
                self.semaphore = threading.Semaphore(max_connections)

            def __enter__(self):
                self.semaphore.acquire()
                return Mock()  # Mock database connection

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.semaphore.release()

        # Test configuration
        num_workers = 6
        operations_per_worker = 500
        connection_pool = MockConnectionPool(max_connections=10)
        results = []

        start_time = time.time()

        # Start concurrent database workers
        threads = []
        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=mock_database_worker,
                args=(worker_id, operations_per_worker, connection_pool, results)
            )
            threads.append(thread)
            thread.start()

        # Wait for all workers to complete
        for thread in threads:
            thread.join()

        total_time = time.time() - start_time

        # Analyze results
        total_operations = sum(r["operations_completed"] for r in results)
        total_errors = sum(r["errors"] for r in results)
        avg_execution_time = sum(r["execution_time"] for r in results) / len(results)
        overall_throughput = total_operations / total_time

        # Performance assertions
        assert len(results) == num_workers
        assert total_operations == num_workers * operations_per_worker
        assert total_errors == 0  # Should have no errors
        assert overall_throughput > 500  # Should achieve > 500 operations/second
        assert total_time < 20  # Should complete within 20 seconds

        print(f"Concurrent DB: {total_operations} ops in {total_time:.2f}s, {overall_throughput:.0f} ops/s")

    def test_thread_safety_validation(self):
        """Test thread safety of shared resources."""

        class ThreadSafeCounter:
            def __init__(self):
                self._value = 0
                self._lock = threading.Lock()

            def increment(self):
                with self._lock:
                    current = self._value
                    time.sleep(0.0001)  # Simulate processing time
                    self._value = current + 1

            @property
            def value(self):
                with self._lock:
                    return self._value

        def worker(counter, iterations):
            """Worker that increments counter."""
            for _ in range(iterations):
                counter.increment()

        # Test configuration
        num_threads = 10
        iterations_per_thread = 100
        counter = ThreadSafeCounter()

        start_time = time.time()

        # Start concurrent threads
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=worker, args=(counter, iterations_per_thread))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        execution_time = time.time() - start_time
        final_value = counter.value
        expected_value = num_threads * iterations_per_thread

        # Thread safety assertions
        assert final_value == expected_value  # Counter should be accurate
        assert execution_time < 5  # Should complete quickly despite locks

        print(f"Thread safety: {final_value}/{expected_value} in {execution_time:.2f}s")

    def test_resource_contention_analysis(self):
        """Test resource contention under high load."""

        def cpu_intensive_worker(worker_id, duration_seconds, results):
            """CPU-intensive worker for contention testing."""
            start_time = time.time()
            operations = 0

            while time.time() - start_time < duration_seconds:
                # CPU-intensive operation
                for i in range(1000):
                    _ = i ** 2
                operations += 1000

            execution_time = time.time() - start_time
            results.append({
                "worker_id": worker_id,
                "operations": operations,
                "execution_time": execution_time,
                "ops_per_second": operations / execution_time
            })

        def io_intensive_worker(worker_id, duration_seconds, results):
            """I/O-intensive worker for contention testing."""
            start_time = time.time()
            operations = 0

            while time.time() - start_time < duration_seconds:
                # Simulate I/O operation
                time.sleep(0.01)
                operations += 1

            execution_time = time.time() - start_time
            results.append({
                "worker_id": worker_id,
                "operations": operations,
                "execution_time": execution_time,
                "ops_per_second": operations / execution_time
            })

        # Test configuration
        duration = 5  # seconds
        cpu_workers = 4
        io_workers = 4
        cpu_results = []
        io_results = []

        start_time = time.time()

        # Start mixed workload
        threads = []

        # CPU-intensive threads
        for i in range(cpu_workers):
            thread = threading.Thread(
                target=cpu_intensive_worker,
                args=(f"cpu_{i}", duration, cpu_results)
            )
            threads.append(thread)
            thread.start()

        # I/O-intensive threads
        for i in range(io_workers):
            thread = threading.Thread(
                target=io_intensive_worker,
                args=(f"io_{i}", duration, io_results)
            )
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        total_time = time.time() - start_time

        # Analyze contention
        cpu_avg_ops = sum(r["ops_per_second"] for r in cpu_results) / len(cpu_results)
        io_avg_ops = sum(r["ops_per_second"] for r in io_results) / len(io_results)

        # Performance assertions
        assert len(cpu_results) == cpu_workers
        assert len(io_results) == io_workers
        assert total_time <= duration + 1  # Should complete close to target duration
        assert cpu_avg_ops > 1000  # CPU workers should achieve reasonable throughput
        assert io_avg_ops > 50  # I/O workers should achieve reasonable throughput

        print(f"Resource contention: CPU={cpu_avg_ops:.0f} ops/s, I/O={io_avg_ops:.0f} ops/s")

    def test_deadlock_prevention(self):
        """Test deadlock prevention in concurrent operations."""

        class Resource:
            def __init__(self, name):
                self.name = name
                self.lock = threading.Lock()

        def ordered_lock_worker(resource1, resource2, worker_id, results):
            """Worker that acquires locks in consistent order to prevent deadlock."""
            # Always acquire locks in alphabetical order by resource name
            first_resource, second_resource = sorted([resource1, resource2], key=lambda r: r.name)

            try:
                acquired_first = first_resource.lock.acquire(timeout=2)
                if acquired_first:
                    try:
                        time.sleep(0.1)  # Simulate work
                        acquired_second = second_resource.lock.acquire(timeout=2)
                        if acquired_second:
                            try:
                                time.sleep(0.1)  # Simulate work with both resources
                                results.append({"worker_id": worker_id, "status": "success"})
                            finally:
                                second_resource.lock.release()
                        else:
                            results.append({"worker_id": worker_id, "status": "timeout_second"})
                    finally:
                        first_resource.lock.release()
                else:
                    results.append({"worker_id": worker_id, "status": "timeout_first"})
            except Exception as e:
                results.append({"worker_id": worker_id, "status": f"error_{str(e)}"})

        # Create resources
        resource_a = Resource("resource_a")
        resource_b = Resource("resource_b")
        results = []

        # Test configuration
        num_workers = 10

        # Start workers that could potentially deadlock
        threads = []
        for i in range(num_workers):
            if i % 2 == 0:
                # Even workers: A then B
                thread = threading.Thread(
                    target=ordered_lock_worker,
                    args=(resource_a, resource_b, i, results)
                )
            else:
                # Odd workers: B then A (but ordered locking prevents deadlock)
                thread = threading.Thread(
                    target=ordered_lock_worker,
                    args=(resource_b, resource_a, i, results)
                )
            threads.append(thread)
            thread.start()

        # Wait for completion with timeout
        start_time = time.time()
        for thread in threads:
            thread.join(timeout=10)  # 10 second timeout

        completion_time = time.time() - start_time

        # Analyze results
        successful_workers = len([r for r in results if r["status"] == "success"])

        # Deadlock prevention assertions
        assert len(results) == num_workers  # All workers should complete
        assert successful_workers == num_workers  # All should succeed
        assert completion_time < 5  # Should complete quickly without deadlock
        assert all(not thread.is_alive() for thread in threads)  # No hanging threads

        print(f"Deadlock prevention: {successful_workers}/{num_workers} succeeded in {completion_time:.2f}s")