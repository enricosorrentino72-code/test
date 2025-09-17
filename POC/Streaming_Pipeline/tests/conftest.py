# Fixed global pytest configuration and fixtures for Databricks Streaming Pipeline Tests

import pytest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, date
import json
import builtins

# Add parent directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =============================================================================
# SETUP DATABRICKS GLOBALS BEFORE ANY IMPORTS
# =============================================================================

def setup_databricks_globals():
    """Set up Databricks global variables"""
    if not hasattr(builtins, 'dbutils'):
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "mock-secret-value"
        mock_dbutils.widgets.get.return_value = "mock-widget-value"
        mock_dbutils.fs.ls.return_value = []
        builtins.dbutils = mock_dbutils

    if not hasattr(builtins, 'spark'):
        mock_spark = MagicMock()
        mock_spark.sql.return_value = MagicMock()
        mock_spark.table.return_value = MagicMock()
        mock_spark.readStream.return_value = MagicMock()
        mock_spark.conf.get.return_value = "test-cluster-id"
        builtins.spark = mock_spark

    if not hasattr(builtins, 'displayHTML'):
        builtins.displayHTML = MagicMock()

# Set up globals immediately
setup_databricks_globals()

# Mock critical modules before any imports
MOCK_MODULES = {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'pyspark.sql.streaming': MagicMock(),
    'databricks': MagicMock(),
    'databricks.labs': MagicMock(),
    'databricks.labs.dqx': MagicMock(),
    'databricks.labs.dqx.engine': MagicMock(),
    'databricks.labs.dqx.rule': MagicMock(),
    'databricks.sdk': MagicMock(),
    'delta': MagicMock(),
    'delta.tables': MagicMock(),
    'azure': MagicMock(),
    'azure.eventhub': MagicMock(),
    'azure.core': MagicMock(),
    'azure.core.exceptions': MagicMock()
}

# Add mocked modules to sys.modules (conditionally skip Azure for cloud tests)
def is_cloud_test_session():
    """Check if we're running cloud tests based on command line arguments."""
    import sys
    # Check if any cloud test files are being run
    for arg in sys.argv:
        if 'cloud_test' in arg:
            return True
    return False

# Only mock Azure packages if we're not running cloud tests
cloud_test_session = is_cloud_test_session()

for module_name, mock_module in MOCK_MODULES.items():
    if module_name not in sys.modules:
        # Skip Azure mocks if running cloud tests
        if cloud_test_session and module_name.startswith('azure'):
            continue
        sys.modules[module_name] = mock_module

# =============================================================================
# PYTEST CONFIGURATION
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers and settings"""
    config.addinivalue_line("markers", "unit: Unit tests (fast, isolated)")
    config.addinivalue_line("markers", "integration: Integration tests (slower, dependencies)")
    config.addinivalue_line("markers", "performance: Performance tests")
    config.addinivalue_line("markers", "critical: Critical priority functions (95% coverage)")
    config.addinivalue_line("markers", "high: High priority functions (95% coverage)")
    config.addinivalue_line("markers", "medium: Medium priority functions (80% coverage)")
    config.addinivalue_line("markers", "low: Low priority functions (60% coverage)")
    config.addinivalue_line("markers", "databricks: Tests requiring Databricks environment")
    config.addinivalue_line("markers", "eventhub: Tests requiring EventHub connectivity")
    config.addinivalue_line("markers", "dqx: Tests for DQX quality framework")
    config.addinivalue_line("markers", "slow: Tests that take a long time to run")
    config.addinivalue_line("markers", "cloud: Cloud integration tests (real Azure services)")

def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names"""
    for item in items:
        if "performance" in item.name.lower():
            item.add_marker(pytest.mark.performance)
        if "integration" in item.name.lower():
            item.add_marker(pytest.mark.integration)
        if "databricks" in item.name.lower():
            item.add_marker(pytest.mark.databricks)
        if "eventhub" in item.name.lower():
            item.add_marker(pytest.mark.eventhub)
        if "dqx" in item.name.lower():
            item.add_marker(pytest.mark.dqx)
        if "slow" in item.name.lower():
            item.add_marker(pytest.mark.slow)

# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="function")
def mock_spark_dataframe():
    """Mock Spark DataFrame for individual tests"""
    mock_df = MagicMock()
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.groupBy.return_value = mock_df
    mock_df.count.return_value = 1000
    mock_df.collect.return_value = []
    mock_df.show.return_value = None
    mock_df.unionByName.return_value = mock_df
    return mock_df

@pytest.fixture(scope="function")
def mock_streaming_query():
    """Mock Spark Streaming Query for tests"""
    mock_query = MagicMock()
    mock_query.id = "test-query-123"
    mock_query.runId = "test-run-456"
    mock_query.isActive = True
    mock_query.lastProgress = {
        'batchId': 1,
        'inputRowsPerSecond': 100.0,
        'processedRowsPerSecond': 95.0,
        'batchDuration': 1000,
        'numInputRows': 100
    }
    mock_query.stop.return_value = None
    return mock_query

# Sample data fixtures
@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing"""
    return {
        "event_id": "weather-event-123",
        "city": "New York",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "temperature": 22.5,
        "humidity": 65.0,
        "wind_speed": 8.2,
        "pressure": 1013.25,
        "precipitation": 0.0,
        "cloud_cover": 25.0,
        "weather_condition": "partly_cloudy",
        "timestamp": "2024-01-15T10:30:00Z",
        "data_source": "databricks_weather_simulator",
        "cluster_id": "cluster-123",
        "notebook_path": "/test/weather"
    }

@pytest.fixture
def sample_bronze_record(sample_weather_data):
    """Sample Bronze layer record for testing"""
    return {
        "record_id": "bronze-123",
        "ingestion_timestamp": datetime.now(),
        "ingestion_date": date.today(),
        "ingestion_hour": 10,
        "eventhub_partition": "0",
        "eventhub_offset": "12345",
        "eventhub_sequence_number": 67890,
        "eventhub_enqueued_time": datetime.now(),
        "eventhub_partition_key": None,
        "processing_cluster_id": "cluster-123",
        "consumer_group": "$Default",
        "eventhub_name": "weather-events",
        "payload_size_bytes": 512,
        "raw_payload": json.dumps(sample_weather_data)
    }

@pytest.fixture
def test_configuration():
    """Test configuration parameters"""
    return {
        'eventhub_scope': 'test-secret-scope',
        'storage_account': 'teststorageaccount',
        'container': 'test-container',
        'bronze_database': 'test_bronze',
        'bronze_table': 'weather_events_raw',
        'silver_database': 'test_silver',
        'silver_table': 'weather_events_silver',
        'quality_criticality': 'error',
        'quality_threshold': 0.95,
        'trigger_mode': '5 seconds',
        'max_events_per_trigger': 1000
    }

# =============================================================================
# UTILITIES
# =============================================================================

class MockDataFrameBuilder:
    """Utility class to build mock DataFrames with specific behaviors"""
    
    def __init__(self):
        self.df = MagicMock()
        
    def with_columns(self, columns):
        """Add columns to mock DataFrame"""
        self.df.columns = columns
        return self
        
    def with_count(self, count):
        """Set count return value"""
        self.df.count.return_value = count
        return self
        
    def with_data(self, data):
        """Set collect return value"""
        self.df.collect.return_value = data
        return self
        
    def build(self):
        """Return the built mock DataFrame"""
        return self.df

def create_mock_streaming_query(query_id="test-query", is_active=True):
    """Utility to create mock streaming queries"""
    mock_query = MagicMock()
    mock_query.id = query_id
    mock_query.runId = f"run-{query_id}"
    mock_query.isActive = is_active
    mock_query.lastProgress = {
        'batchId': 1,
        'inputRowsPerSecond': 100.0,
        'processedRowsPerSecond': 95.0
    }
    return mock_query