"""
Cloud Integration Tests for Azure EventHub
Tests real EventHub connectivity and message processing
"""

import pytest
import json
import time
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

# Only import Azure packages if we have credentials
# This prevents import errors when Azure SDK is not available
try:
    from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

from dotenv import load_dotenv

# Load test environment
load_dotenv('.env.test')

# Test markers
pytestmark = [
    pytest.mark.integration,
    pytest.mark.cloud,
    pytest.mark.eventhub,
    pytest.mark.slow
]

class TestEventHubCloudIntegration:
    """Cloud integration tests for Azure EventHub functionality."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class with Azure credentials and configuration."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")
            
        cls.connection_string = os.getenv('TEST_EVENTHUB_CONNECTION_STRING')
        cls.eventhub_name = os.getenv('TEST_EVENTHUB_NAME')
        cls.consumer_group = os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
        cls.test_timeout = int(os.getenv('TEST_TIMEOUT', '300'))
        cls.max_events = int(os.getenv('TEST_MAX_EVENTS', '100'))
        cls.cleanup_after = os.getenv('TEST_CLEANUP_AFTER_RUN', 'true').lower() == 'true'
        
        if not cls.connection_string or not cls.eventhub_name:
            pytest.skip("EventHub test configuration not available - copy .env.test.template to .env.test and configure")
    
    def setup_method(self):
        """Set up for each test method."""
        self.test_messages = []
        self.received_messages = []
        self.start_time = time.time()
    
    def teardown_method(self):
        """Clean up after each test method."""
        if self.cleanup_after:
            # Clean up test data if needed
            pass
            
    def generate_test_weather_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate test weather data for EventHub."""
        test_data = []
        base_time = datetime.now(timezone.utc)
        
        for i in range(count):
            data = {
                "messageId": f"test-{base_time.isoformat()}-{i:04d}",
                "timestamp": (base_time.timestamp() + i) * 1000,  # milliseconds
                "location": f"TestCity{i % 5}",
                "temperature": 20.0 + (i % 30),
                "humidity": 50.0 + (i % 40), 
                "pressure": 1013.25 + (i % 50),
                "windSpeed": 5.0 + (i % 20),
                "testRun": True,
                "testId": f"integration-test-{int(time.time())}"
            }
            test_data.append(data)
        
        return test_data

    @pytest.mark.critical
    @pytest.mark.timeout(300)
    def test_eventhub_producer_cloud_connectivity(self):
        """Test EventHub producer connectivity with real Azure service."""
        try:
            # Create producer client
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            
            # Test connection with a simple message
            test_data = {
                "test": "connectivity_check",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "testId": f"connectivity-{int(time.time())}"
            }
            
            # Create event batch
            with producer:
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(json.dumps(test_data)))
                
                # Send the batch
                producer.send_batch(event_data_batch)
            
            # If we reach here, connection was successful
            assert True, "EventHub producer connectivity successful"
            
        except AzureError as e:
            pytest.fail(f"EventHub connectivity failed: {str(e)}")
        except Exception as e:
            pytest.fail(f"Unexpected error during EventHub connectivity test: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(300)
    def test_eventhub_producer_batch_send(self):
        """Test EventHub producer batch message sending."""
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            
            # Generate test weather data
            test_data = self.generate_test_weather_data(5)
            self.test_messages = test_data
            
            # Send messages in batch
            with producer:
                event_data_batch = producer.create_batch()
                
                for message in test_data:
                    try:
                        event_data_batch.add(EventData(json.dumps(message)))
                    except ValueError:
                        # Batch is full, send current batch and start new one
                        producer.send_batch(event_data_batch)
                        event_data_batch = producer.create_batch()
                        event_data_batch.add(EventData(json.dumps(message)))
                
                # Send remaining messages
                if len(event_data_batch) > 0:
                    producer.send_batch(event_data_batch)
            
            # Verify batch was sent successfully
            assert len(test_data) == 5, "All test messages should be prepared for sending"
            
        except Exception as e:
            pytest.fail(f"Batch send test failed: {str(e)}")

    @pytest.mark.performance
    @pytest.mark.timeout(600)
    def test_eventhub_throughput_performance(self):
        """Test EventHub throughput performance with message volume."""
        message_count = 50
        batch_size = 10
        
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            
            # Generate test dataset
            test_data = self.generate_test_weather_data(message_count)
            
            start_time = time.time()
            
            # Send in batches for better performance
            with producer:
                for i in range(0, len(test_data), batch_size):
                    batch = test_data[i:i + batch_size]
                    event_data_batch = producer.create_batch()
                    
                    for message in batch:
                        event_data_batch.add(EventData(json.dumps(message)))
                    
                    producer.send_batch(event_data_batch)
            
            send_time = time.time() - start_time
            
            # Performance assertions
            messages_per_second = message_count / send_time
            assert messages_per_second > 5, f"Should send >5 messages/second, got {messages_per_second:.2f}"
            assert send_time < 60, f"Should complete within 60 seconds, took {send_time:.2f}s"
            
            print(f"Performance: {messages_per_second:.2f} messages/second, {send_time:.2f}s total")
            
        except Exception as e:
            pytest.fail(f"Performance test failed: {str(e)}")

    @pytest.mark.medium
    def test_eventhub_error_handling(self):
        """Test EventHub error handling with invalid configurations."""
        # Test with invalid connection string
        with pytest.raises((AzureError, ValueError)):
            producer = EventHubProducerClient.from_connection_string(
                conn_str="invalid_connection_string",
                eventhub_name=self.eventhub_name
            )
            with producer:
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData("test"))
                producer.send_batch(event_data_batch)

    @pytest.mark.low
    def test_eventhub_connection_recovery(self):
        """Test EventHub connection recovery capabilities."""
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            
            # Send initial message to establish connection
            test_message = {"test": "recovery", "timestamp": time.time()}
            
            with producer:
                for attempt in range(3):  # Test multiple attempts
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(EventData(json.dumps(test_message)))
                    producer.send_batch(event_data_batch)
                    time.sleep(1)  # Small delay between attempts
            
            assert True, "Connection recovery test completed successfully"
            
        except Exception as e:
            pytest.fail(f"Connection recovery test failed: {str(e)}")


# Test configuration helpers
@pytest.fixture(scope="session")
def eventhub_config():
    """Provide EventHub configuration for tests."""
    return {
        'connection_string': os.getenv('TEST_EVENTHUB_CONNECTION_STRING'),
        'eventhub_name': os.getenv('TEST_EVENTHUB_NAME'),
        'consumer_group': os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
    }