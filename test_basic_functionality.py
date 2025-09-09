#!/usr/bin/env python3
"""
Basic functionality test script for the Event Hub Delta Pipeline.
This script tests the core components without requiring actual Azure resources.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test that all modules can be imported successfully."""
    print("Testing module imports...")
    
    try:
        from config.settings import get_settings, EventHubSettings, ADLSSettings
        print("✓ Configuration module imported successfully")
        
        from monitoring.logger import configure_logging, get_logger, PipelineLogger
        print("✓ Logging module imported successfully")
        
        from monitoring.metrics import PipelineMetrics
        print("✓ Metrics module imported successfully")
        
        from monitoring.health_check import HealthChecker
        print("✓ Health check module imported successfully")
        
        print("✓ All core modules imported successfully")
        return True
        
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


def test_configuration():
    """Test configuration loading with environment variables."""
    print("\nTesting configuration...")
    
    try:
        os.environ.update({
            'EVENTHUB_CONNECTION_STRING': 'test-connection-string',
            'EVENTHUB_EVENT_HUB_NAME': 'test-hub',
            'ADLS_ACCOUNT_NAME': 'test-account',
            'ADLS_CONTAINER_NAME': 'test-container'
        })
        
        from config.settings import get_settings
        
        settings = get_settings()
        
        assert settings.eventhub.connection_string == 'test-connection-string'
        assert settings.eventhub.event_hub_name == 'test-hub'
        assert settings.adls.account_name == 'test-account'
        assert settings.adls.container_name == 'test-container'
        
        print("✓ Configuration loading works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False


def test_logging():
    """Test logging configuration."""
    print("\nTesting logging...")
    
    try:
        from monitoring.logger import configure_logging, get_logger, PipelineLogger
        
        configure_logging("INFO")
        
        logger = get_logger("test")
        logger.info("Test log message")
        
        pipeline_logger = PipelineLogger("test_component")
        pipeline_logger.info("Test pipeline log message")
        
        print("✓ Logging configuration works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Logging test failed: {e}")
        return False


def test_metrics():
    """Test metrics collection (without starting HTTP server)."""
    print("\nTesting metrics...")
    
    try:
        from monitoring.metrics import PipelineMetrics
        
        metrics = PipelineMetrics(enable_metrics=False)
        
        print("✓ Metrics module works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Metrics test failed: {e}")
        return False


def test_health_check():
    """Test health check functionality (without starting HTTP server)."""
    print("\nTesting health checks...")
    
    try:
        from monitoring.health_check import HealthChecker
        
        health_checker = HealthChecker()
        
        health_status = health_checker.get_health_status()
        
        assert "healthy" in health_status
        assert "checks" in health_status
        
        print("✓ Health check functionality works correctly")
        return True
        
    except Exception as e:
        print(f"✗ Health check test failed: {e}")
        return False


def test_spark_dependencies():
    """Test Spark-related dependencies (if available)."""
    print("\nTesting Spark dependencies...")
    
    try:
        import pyspark
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType
        
        print("✓ PySpark is available")
        
        try:
            from delta.tables import DeltaTable
            print("✓ Delta Lake is available")
        except ImportError:
            print("⚠ Delta Lake not available (expected in basic environment)")
        
        return True
        
    except ImportError as e:
        print(f"⚠ Spark dependencies not available: {e}")
        print("  This is expected if running outside of a Spark environment")
        return True  # Not a failure for basic testing


def test_azure_dependencies():
    """Test Azure SDK dependencies."""
    print("\nTesting Azure dependencies...")
    
    try:
        import azure.eventhub
        print("✓ Azure Event Hub SDK is available")
        
        import azure.identity
        print("✓ Azure Identity SDK is available")
        
        import azure.storage.filedatalake
        print("✓ Azure Storage SDK is available")
        
        return True
        
    except ImportError as e:
        print(f"⚠ Azure dependencies not fully available: {e}")
        print("  Install with: pip install -r requirements.txt")
        return False


def main():
    """Run all basic functionality tests."""
    print("Event Hub Delta Pipeline - Basic Functionality Tests")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_configuration,
        test_logging,
        test_metrics,
        test_health_check,
        test_spark_dependencies,
        test_azure_dependencies
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print(f"\nTest Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The pipeline components are working correctly.")
        return True
    else:
        print("⚠ Some tests failed. Please check the output above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
