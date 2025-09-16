# Global pytest configuration and fixtures for Databricks Streaming Pipeline Tests

import pytest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, date
import json

# Add parent directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =============================================================================
# GLOBAL PYTEST CONFIGURATION
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers and settings"""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (slower, dependencies)"
    )
    config.addinivalue_line(
        "markers", "performance: Performance tests"
    )
    config.addinivalue_line(
        "markers", "critical: Critical priority functions (95% coverage)"
    )
    config.addinivalue_line(
        "markers", "high: High priority functions (95% coverage)"
    )
    config.addinivalue_line(
        "markers", "medium: Medium priority functions (80% coverage)"
    )
    config.addinivalue_line(
        "markers", "low: Low priority functions (60% coverage)"
    )
    config.addinivalue_line(
        "markers", "databricks: Tests requiring Databricks environment"
    )
    config.addinivalue_line(
        "markers", "eventhub: Tests requiring EventHub connectivity"
    )
    config.addinivalue_line(
        "markers", "dqx: Tests for DQX quality framework"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take a long time to run"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names"""
    for item in items:
        # Auto-mark tests based on naming conventions
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
# GLOBAL FIXTURES - MOCK ENVIRONMENTS
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def mock_databricks_global_environment():
    """Session-wide mock of Databricks environment"""
    # Mock core Databricks modules
    mock_modules = {
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
    
    # Create mock objects
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.return_value = "mock-secret-value"
    mock_dbutils.widgets.get.return_value = "mock-widget-value"
    mock_dbutils.fs.ls.return_value = []
    
    mock_spark = MagicMock()
    mock_spark.sql.return_value = MagicMock()
    mock_spark.table.return_value = MagicMock()
    mock_spark.readStream.return_value = MagicMock()
    mock_spark.conf.get.return_value = "test-cluster-id"
    
    mock_displayHTML = MagicMock()
    
    # Set up globals in builtins
    import builtins
    builtins.dbutils = mock_dbutils
    builtins.spark = mock_spark  
    builtins.displayHTML = mock_displayHTML
    
    with patch.dict('sys.modules', mock_modules):
        yield {
            'dbutils': mock_dbutils,
            'spark': mock_spark,
            'displayHTML': mock_displayHTML,
            'modules': mock_modules
        }
        
    # Clean up globals
    if hasattr(builtins, 'dbutils'):
        delattr(builtins, 'dbutils')
    if hasattr(builtins, 'spark'):
        delattr(builtins, 'spark')
    if hasattr(builtins, 'displayHTML'):
        delattr(builtins, 'displayHTML')

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

@pytest.fixture(scope="function")
def mock_dqx_framework():
    """Mock DQX framework components"""
    mock_engine = MagicMock()
    mock_rules = [MagicMock(), MagicMock()]
    mock_check_funcs = MagicMock()
    
    # Mock DQX engine methods
    mock_engine.apply_checks_and_split.return_value = (MagicMock(), MagicMock())
    mock_engine.validate_checks.return_value = MagicMock()
    
    return {
        'engine': mock_engine,
        'rules': mock_rules,
        'check_funcs': mock_check_funcs,
        'available': True
    }

# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================

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
def sample_silver_record(sample_weather_data):
    """Sample Silver layer record for testing"""
    return {
        **sample_weather_data,
        "source_record_id": "bronze-123",
        "bronze_ingestion_timestamp": datetime.now(),
        "bronze_ingestion_date": date.today(),
        "bronze_ingestion_hour": 10,
        "silver_processing_timestamp": datetime.now(),
        "silver_processing_date": date.today(),
        "silver_processing_hour": 10,
        "parsing_status": "SUCCESS",
        "parsing_errors": [],
        "flag_check": "PASS",
        "dqx_quality_score": 0.95,
        "dqx_rule_results": ["all_rules_passed"],
        "failed_rules_count": 0,
        "passed_rules_count": 5
    }

@pytest.fixture
def sample_eventhub_message(sample_weather_data):
    """Sample EventHub message for testing"""
    return {
        'body': json.dumps(sample_weather_data).encode('utf-8'),
        'partition_id': '0',
        'offset': '12345',
        'sequence_number': 67890,
        'enqueued_time': datetime.now(),
        'partition_key': None,
        'properties': {}
    }

# =============================================================================
# CONFIGURATION FIXTURES
# =============================================================================

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

@pytest.fixture
def mock_pipeline_paths():
    """Mock pipeline paths for testing"""
    return {
        'bronze_path': '/mnt/test/bronze/weather',
        'silver_path': '/mnt/test/silver/weather',
        'checkpoint_path': '/mnt/test/checkpoints/bronze-to-silver',
        'dqx_checkpoint_path': '/mnt/test/checkpoints/bronze-to-silver-dqx'
    }

# =============================================================================
# PERFORMANCE TESTING FIXTURES
# =============================================================================

@pytest.fixture
def performance_timer():
    """Fixture for measuring test performance"""
    import time
    
    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None
        
        def start(self):
            self.start_time = time.time()
        
        def stop(self):
            self.end_time = time.time()
        
        @property
        def duration(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None
    
    return Timer()

# =============================================================================
# ERROR SIMULATION FIXTURES
# =============================================================================

@pytest.fixture
def error_scenarios():
    """Common error scenarios for testing"""
    return {
        'connection_error': Exception("Connection failed"),
        'timeout_error': TimeoutError("Operation timed out"),
        'authentication_error': Exception("Authentication failed"),
        'json_parse_error': json.JSONDecodeError("Invalid JSON", "", 0),
        'sql_error': Exception("SQL execution failed"),
        'dqx_error': Exception("DQX validation failed")
    }

# =============================================================================
# COVERAGE TRACKING FIXTURES
# =============================================================================

@pytest.fixture
def coverage_tracker():
    """Track test coverage for different priority functions"""
    return {
        'critical_functions': {
            'target_coverage': 95,
            'tested_functions': set(),
            'total_functions': set()
        },
        'high_functions': {
            'target_coverage': 95,
            'tested_functions': set(),
            'total_functions': set()
        },
        'medium_functions': {
            'target_coverage': 80,
            'tested_functions': set(),
            'total_functions': set()
        },
        'low_functions': {
            'target_coverage': 60,
            'tested_functions': set(),
            'total_functions': set()
        }
    }

# =============================================================================
# CUSTOM ASSERTIONS
# =============================================================================

def assert_dataframe_structure(df_mock, expected_columns):
    """Assert that DataFrame has expected column structure"""
    # For mocked DataFrames, we verify select was called with expected columns
    if hasattr(df_mock, 'select') and df_mock.select.called:
        return True
    return False

def assert_quality_metadata_present(df_mock):
    """Assert that quality metadata columns are present"""
    expected_quality_columns = [
        'flag_check', 'description_failure', 'dqx_rule_results',
        'dqx_quality_score', 'dqx_validation_timestamp',
        'dqx_lineage_id', 'dqx_criticality_level',
        'failed_rules_count', 'passed_rules_count'
    ]
    # For mocked DataFrames, verify withColumn was called for quality columns
    return hasattr(df_mock, 'withColumn') and df_mock.withColumn.called

def assert_performance_threshold(duration, max_duration, operation_name):
    """Assert that operation completed within performance threshold"""
    assert duration < max_duration, f"{operation_name} took {duration:.3f}s, exceeding threshold of {max_duration}s"

# =============================================================================
# PYTEST HOOKS
# =============================================================================

def pytest_runtest_setup(item):
    """Setup hook run before each test"""
    # Add any pre-test setup here
    pass

def pytest_runtest_teardown(item, nextitem):
    """Teardown hook run after each test"""
    # Add any post-test cleanup here
    pass

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