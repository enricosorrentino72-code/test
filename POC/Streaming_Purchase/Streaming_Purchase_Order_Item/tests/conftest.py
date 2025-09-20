# Test fixtures and configuration for Purchase Order Item Pipeline
import pytest
import asyncio
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
import sys
import os

# Add project root to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Mock Databricks modules before any imports
class MockDBUtils:
    class widgets:
        @staticmethod
        def get(key):
            default_values = {
                "eventhub_scope": "test-scope",
                "eventhub_name": "purchase-order-items-test",
                "batch_size": "10",
                "send_interval": "1.0",
                "duration_minutes": "1",
                "log_level": "INFO",
                "consumer_group": "$Default",
                "bronze_path": "/tmp/test-bronze",
                "checkpoint_path": "/tmp/test-checkpoint",
                "storage_account": "testaccount",
                "container": "test-container",
                "database_name": "test_db",
                "table_name": "test_table",
                "trigger_mode": "1 second",
                "max_events_per_trigger": "100",
                "quality_criticality": "error",
                "quality_threshold": "0.95"
            }
            return default_values.get(key, "default_value")

        @staticmethod
        def text(key, default, description):
            pass

        @staticmethod
        def dropdown(key, default, options, description):
            pass

    class secrets:
        @staticmethod
        def get(scope, key):
            return "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=testkey"

    class notebook:
        @staticmethod
        def entry_point():
            mock_entry = Mock()
            mock_entry.getDbutils.return_value.notebook.return_value.getContext.return_value.notebookPath.return_value.get.return_value = "/test/notebook/path"
            return mock_entry

class MockSpark:
    class conf:
        @staticmethod
        def get(key):
            return "test-cluster-id"

        @staticmethod
        def set(key, value):
            pass

    class sql:
        @staticmethod
        def sql(query):
            return Mock()

    @staticmethod
    def readStream():
        return Mock()

    @staticmethod
    def table(name):
        return Mock()

# Set up mock globals before imports
import builtins
builtins.dbutils = MockDBUtils()
builtins.spark = MockSpark()

# Fixture for async tests
@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_purchase_order_data():
    """Sample purchase order data for testing"""
    return {
        "order_id": "ORD-123456",
        "product_id": "PRD001",
        "product_name": "Test Laptop",
        "quantity": 2,
        "unit_price": 1000.00,
        "total_amount": 2000.00,
        "customer_id": "CUST-1234",
        "vendor_id": "VEND-123",
        "warehouse_location": "WH-NYC",
        "currency": "USD",
        "order_status": "NEW",
        "payment_status": "PENDING",
        "timestamp": datetime.utcnow(),
        "created_at": datetime.utcnow() - timedelta(hours=1)
    }

@pytest.fixture
def mock_eventhub_client():
    """Mock EventHub client for testing"""
    with patch('azure.eventhub.EventHubProducerClient.from_connection_string') as mock_client:
        client_instance = Mock()
        mock_client.return_value = client_instance

        # Mock batch creation and sending
        mock_batch = Mock()
        client_instance.create_batch.return_value = mock_batch
        client_instance.send_batch.return_value = None
        mock_batch.add.return_value = None

        yield client_instance

@pytest.fixture
def mock_dqx_engine():
    """Mock DQX engine for testing"""
    with patch('databricks.labs.dqx.engine.DQEngine') as mock_engine_class:
        mock_engine = Mock()
        mock_engine_class.return_value = mock_engine

        # Mock DQX methods
        mock_engine.apply_checks_and_split.return_value = (Mock(), Mock())  # valid_df, invalid_df

        yield mock_engine

@pytest.fixture
def mock_spark_session():
    """Mock Spark session for testing"""
    with patch('pyspark.sql.SparkSession.builder') as mock_builder:
        mock_session = Mock()
        mock_builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session

        # Mock DataFrame operations
        mock_df = Mock()
        mock_session.readStream.format.return_value.table.return_value = mock_df
        mock_session.sql.return_value = Mock()

        yield mock_session

@pytest.fixture
def sample_purchase_orders():
    """Generate sample purchase order items for testing"""
    orders = []
    for i in range(5):
        orders.append({
            "order_id": f"ORD-{100000 + i}",
            "product_id": f"PRD00{i + 1}",
            "product_name": f"Test Product {i + 1}",
            "quantity": i + 1,
            "unit_price": 100.0 * (i + 1),
            "total_amount": 100.0 * (i + 1) * (i + 1),
            "customer_id": f"CUST-{1000 + i}",
            "vendor_id": f"VEND-{100 + i}",
            "warehouse_location": f"WH-TEST{i + 1}",
            "currency": "USD",
            "order_status": "NEW",
            "payment_status": "PENDING",
            "timestamp": datetime.utcnow() - timedelta(hours=i),
            "created_at": datetime.utcnow() - timedelta(hours=i + 1)
        })
    return orders

@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mocks between tests"""
    yield
    # Any cleanup can go here

# Performance testing fixtures
@pytest.fixture
def performance_thresholds():
    """Define performance thresholds for benchmarking"""
    return {
        "producer_throughput": 8000,  # records/second
        "dqx_processing_speed": 20000,  # rule evaluations/second
        "memory_increase_limit": 150,  # MB
        "transformation_time": 0.3  # seconds per 1K records
    }

@pytest.fixture
def dqx_quality_scenarios():
    """Generate purchase orders with various quality issues"""
    scenarios = {
        "valid": {
            "order_id": "ORD-123456",
            "product_id": "PRD001",
            "quantity": 2,
            "unit_price": 100.0,
            "total_amount": 200.0,
            "customer_id": "CUST-1234"
        },
        "invalid_calculation": {
            "order_id": "ORD-123457",
            "product_id": "PRD002",
            "quantity": 2,
            "unit_price": 100.0,
            "total_amount": 150.0,  # Wrong calculation
            "customer_id": "CUST-1235"
        },
        "zero_quantity": {
            "order_id": "ORD-123458",
            "product_id": "PRD003",
            "quantity": 0,  # Invalid
            "unit_price": 100.0,
            "total_amount": 0.0,
            "customer_id": "CUST-1236"
        },
        "negative_price": {
            "order_id": "ORD-123459",
            "product_id": "PRD004",
            "quantity": 1,
            "unit_price": -50.0,  # Invalid
            "total_amount": -50.0,
            "customer_id": "CUST-1237"
        }
    }
    return scenarios