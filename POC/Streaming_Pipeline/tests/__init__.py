# Databricks Streaming Pipeline Test Suite
# 
# Comprehensive test coverage for:
# - EventHub Producer and Listener components
# - Bronze to Silver data transformation pipeline  
# - DQX quality framework integration
# - Hive Metastore management
#
# Coverage Goals:
# - CRITICAL Priority: 95% coverage
# - HIGH Priority: 95% coverage
# - MEDIUM Priority: 80% coverage
# - LOW Priority: 60% coverage
# - Overall Target: 80% minimum

__version__ = "1.0.0"
__author__ = "Databricks Streaming Pipeline Team"
__description__ = "Test suite for Databricks streaming data pipeline with DQX quality framework"

# Test suite metadata
TEST_SUITE_INFO = {
    "version": __version__,
    "total_test_files": 3,
    "total_functions_tested": 28,
    "coverage_targets": {
        "critical": 95,
        "high": 95, 
        "medium": 80,
        "low": 60,
        "overall": 80
    },
    "test_categories": [
        "unit",
        "integration", 
        "performance",
        "critical",
        "high",
        "medium", 
        "low",
        "databricks",
        "eventhub",
        "dqx"
    ]
}

# Export key testing utilities
from .conftest import (
    MockDataFrameBuilder,
    create_mock_streaming_query
)

__all__ = [
    "TEST_SUITE_INFO",
    "MockDataFrameBuilder", 
    "create_mock_streaming_query"
]