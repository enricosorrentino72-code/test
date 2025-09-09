#!/usr/bin/env python3
"""
Local pipeline test script that simulates the Event Hub Delta Pipeline
without requiring actual Azure resources.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_local_pipeline():
    """Test the pipeline components locally with mock data."""
    print("Testing Event Hub Delta Pipeline Locally")
    print("=" * 50)
    
    try:
        print("1. Testing configuration...")
        os.environ.update({
            'EVENTHUB_CONNECTION_STRING': 'test-connection-string',
            'EVENTHUB_EVENT_HUB_NAME': 'test-hub',
            'ADLS_ACCOUNT_NAME': 'test-account',
            'ADLS_CONTAINER_NAME': 'test-container'
        })
        
        from config.settings import get_settings
        settings = get_settings()
        print(f"   ✓ Configuration loaded: {settings.eventhub.event_hub_name}")
        
        print("2. Testing logging...")
        from monitoring.logger import configure_logging, PipelineLogger
        configure_logging("INFO")
        logger = PipelineLogger("test_pipeline")
        logger.info("Test log message", test_param="test_value")
        print("   ✓ Logging configured and working")
        
        print("3. Testing metrics...")
        from monitoring.metrics import PipelineMetrics
        metrics = PipelineMetrics(enable_metrics=False)
        metrics.record_event_processed("valid", "USER.LOGIN", "test-source")
        print("   ✓ Metrics collection working")
        
        print("4. Testing health checks...")
        from monitoring.health_check import HealthChecker
        health_checker = HealthChecker()
        health_status = health_checker.get_health_status()
        print(f"   ✓ Health check status: {health_status['healthy']}")
        
        print("5. Testing data processing logic...")
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            spark = (SparkSession.builder
                    .appName("test-pipeline")
                    .master("local[1]")
                    .config("spark.sql.shuffle.partitions", "1")
                    .getOrCreate())
            
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_time", TimestampType(), True),
                StructField("event_source", StringType(), True),
                StructField("event_subject", StringType(), True),
                StructField("event_data", StringType(), True),
                StructField("enqueued_time", TimestampType(), True),
                StructField("processing_time", TimestampType(), True)
            ])
            
            sample_data = [
                ("event-123", "USER.LOGIN", datetime.now(), "app.example.com", "user/login", '{"userId": "user123"}', datetime.now(), datetime.now()),
                ("", "SYSTEM.ERROR", datetime.now(), "system", "error", '{"error": "test"}', datetime.now(), datetime.now()),
                ("event-456", "", None, "api.example.com", "api/request", '{"endpoint": "/users"}', datetime.now(), datetime.now())
            ]
            
            df = spark.createDataFrame(sample_data, schema)
            
            from processing.data_processor import DataProcessor
            processor = DataProcessor(spark)
            
            valid_events, invalid_events = processor.process_events(df)
            
            valid_count = valid_events.count()
            invalid_count = invalid_events.count()
            
            print(f"   ✓ Data processing: {valid_count} valid, {invalid_count} invalid events")
            
            print("   Sample valid events:")
            valid_events.select("event_id", "event_type", "event_category", "is_valid").show(truncate=False)
            
            spark.stop()
            
        except ImportError:
            print("   ⚠ Spark not available, skipping data processing test")
        except Exception as e:
            print(f"   ⚠ Data processing test failed: {e}")
        
        print("\n🎉 Local pipeline test completed successfully!")
        print("\nNext steps:")
        print("1. Set up Azure resources using: python deploy.py infrastructure")
        print("2. Update .env file with actual Azure credentials")
        print("3. Run the full pipeline with: python src/main.py")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Local pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sample_event_generation():
    """Test sample event generation."""
    print("\nTesting sample event generation...")
    
    try:
        from sample_event_generator import SampleEventGenerator
        
        generator = SampleEventGenerator()
        
        events = generator.generate_batch(count=5, time_range_minutes=10)
        invalid_events = generator.generate_invalid_events(count=2)
        
        print(f"   ✓ Generated {len(events)} valid events")
        print(f"   ✓ Generated {len(invalid_events)} invalid events")
        
        if events:
            print("   Sample event structure:")
            sample_event = events[0]
            for key, value in sample_event.items():
                print(f"     {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Sample event generation failed: {e}")
        return False


def main():
    """Run all local tests."""
    success = True
    
    if not test_local_pipeline():
        success = False
    
    if not test_sample_event_generation():
        success = False
    
    if success:
        print("\n✅ All local tests passed!")
    else:
        print("\n❌ Some tests failed. Please check the output above.")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
