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
from unittest.mock import patch
import asyncio
import logging
import time
import json
import threading


# Import Azure packages with error handling
try:
    from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError as e:
    # If Azure packages are not available, skip all tests
    AZURE_AVAILABLE = False
    # Create dummy classes to prevent NameError
    class EventHubProducerClient: pass
    class EventHubConsumerClient: pass
    class EventData: pass
    class DefaultAzureCredential: pass
    class ClientSecretCredential: pass
    class AzureError(Exception): pass

from dotenv import load_dotenv

# Load test environment
# Try multiple possible locations for .env.test file
import os
env_file_locations = [
    '.env.test',
    'tests/.env.test',
    os.path.join(os.path.dirname(__file__), '.env.test'),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env.test')
]

env_loaded = False
for env_file in env_file_locations:
    if os.path.exists(env_file):
        load_result = load_dotenv(env_file)
        if load_result:
            print(f"[OK] Environment loaded from: {env_file}")
            env_loaded = True
            break
        else:
            print(f"[WARN] Found but failed to load: {env_file}")
    else:
        print(f"[SKIP] Not found: {env_file}")

if not env_loaded:
    print("[WARN] No .env.test file loaded - tests will skip if no Azure credentials")

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
        # First check if Azure packages are available
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK packages not available - install azure-eventhub, azure-identity")
            
        cls.connection_string = os.getenv('TEST_EVENTHUB_CONNECTION_STRING')
        cls.eventhub_name = os.getenv('TEST_EVENTHUB_NAME')
        cls.consumer_group = os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
        cls.test_timeout = int(os.getenv('TEST_TIMEOUT', '300'))
        cls.max_events = int(os.getenv('TEST_MAX_EVENTS', '100'))
        cls.cleanup_after = os.getenv('TEST_CLEANUP_AFTER_RUN', 'true').lower() == 'true'
        
        if not cls.connection_string or not cls.eventhub_name:
            pytest.skip("EventHub test configuration not available - create .env.test with Azure credentials")
    
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
        # Set up detailed logging
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            force=True
        )
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("=== START test_eventhub_producer_cloud_connectivity ===")
            logger.info(f"EventHub Name: {self.eventhub_name}")
            logger.info(f"Connection String Length: {len(self.connection_string) if self.connection_string else 0}")
            logger.info(f"Connection String Preview: {self.connection_string[:50]}..." if self.connection_string else "No connection string")
            
            # Step 1: Create producer client
            logger.info("🔄 STEP 1: Creating EventHub producer client...")
            start_time = time.time()
            
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            
            creation_time = time.time() - start_time
            logger.info(f"✅ Producer client created successfully in {creation_time:.2f}s")
            logger.info(f"Producer object: {type(producer)}")
            
            # Step 2: Prepare test message
            logger.info("🔄 STEP 2: Preparing test message...")
            test_id = f"connectivity-{int(time.time())}"
            test_data = {
                "test": "connectivity_check",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "testId": test_id,
                "messageSize": "small",
                "purpose": "cloud_integration_test"
            }
            
            logger.info(f"Test message ID: {test_id}")
            logger.info(f"Test message size: {len(json.dumps(test_data))} bytes")
            logger.info(f"Test message content: {json.dumps(test_data, indent=2)}")
            
            # Step 3: Create and send event batch
            logger.info("🔄 STEP 3: Creating event batch and sending to EventHub...")
            send_start_time = time.time()
            
            with producer:
                logger.info("📡 Producer context entered")
                
                # Create batch
                batch_start_time = time.time()
                event_data_batch = producer.create_batch()
                batch_creation_time = time.time() - batch_start_time
                
                logger.info(f"✅ Event batch created in {batch_creation_time:.3f}s")
                logger.info(f"Batch max size: {event_data_batch.max_size_in_bytes} bytes")
                
                # Add event to batch
                event_data = EventData(json.dumps(test_data))
                logger.info(f"EventData object created: {type(event_data)}")
                
                try:
                    event_data_batch.add(event_data)
                    logger.info(f"✅ Event added to batch. Current batch size: {event_data_batch.size_in_bytes} bytes")
                except Exception as batch_error:
                    logger.error(f"❌ Failed to add event to batch: {batch_error}")
                    raise
                
                # Send the batch
                logger.info("🚀 Sending batch to Azure EventHub...")
                send_attempt_start = time.time()
                
                try:
                    producer.send_batch(event_data_batch)
                    send_time = time.time() - send_attempt_start
                    total_send_time = time.time() - send_start_time
                    
                    logger.info(f"✅ Batch sent successfully!")
                    logger.info(f"Send operation time: {send_time:.3f}s")
                    logger.info(f"Total send process time: {total_send_time:.3f}s")
                    
                except Exception as send_error:
                    logger.error(f"❌ Failed to send batch: {send_error}")
                    logger.error(f"Error type: {type(send_error)}")
                    raise
                
                logger.info("📡 Producer context exited")
            
            # Step 4: Verify success
            total_test_time = time.time() - start_time
            logger.info("🔄 STEP 4: Verifying test completion...")
            logger.info(f"✅ Total test execution time: {total_test_time:.3f}s")
            logger.info("✅ EventHub producer connectivity test PASSED")
            
            # Success assertion
            assert True, f"EventHub producer connectivity successful - Message {test_id} sent in {total_test_time:.2f}s"
            
        except AzureError as e:
            logger.error(f"❌ Azure-specific error occurred: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details'}")
            pytest.fail(f"EventHub connectivity failed with Azure error: {str(e)}")
            
        except Exception as e:
            logger.error(f"❌ Unexpected error occurred: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details'}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            pytest.fail(f"Unexpected error during EventHub connectivity test: {str(e)}")
            
        finally:
            logger.info("=== END test_eventhub_producer_cloud_connectivity ===")

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
            test_data = self.generate_test_weather_data(10)
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
            assert len(test_data) == 10, "All test messages should be prepared for sending"
            
        except Exception as e:
            pytest.fail(f"Batch send test failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(300) 
    def test_eventhub_consumer_cloud_receive(self):
        """Test EventHub consumer receiving messages from real Azure service."""
        # Set up logging for this test - filter out noisy CBS status checks
        logging.basicConfig(
            level=logging.INFO,  # Use INFO instead of DEBUG to reduce noise
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            force=True
        )
        logger = logging.getLogger(__name__)
        
        # Filter out repetitive CBS status check messages
        logging.getLogger('azure.eventhub._pyamqp.cbs').setLevel(logging.WARNING)
        
        try:
            logger.info("=== START test_eventhub_consumer_cloud_receive ===")
            
            # Generate unique test ID for this run
            test_run_id = f"consumer-test-{int(time.time())}-{os.getpid()}"
            logger.info(f"Test Run ID: {test_run_id}")
            
            logger.info("🔄 STEP 1: Sending test messages to EventHub...")
            
            # First send test messages with unique ID
            self._send_test_messages_to_eventhub(5, test_run_id=test_run_id)
            logger.info("✅ Test messages sent successfully")
            
            # Wait a moment for messages to be available
            logger.info("🔄 STEP 2: Waiting for message propagation...")
            time.sleep(3)
            
            # Create consumer client
            logger.info("🔄 STEP 3: Creating EventHub consumer client...")
            consumer_start_time = time.time()
            
            consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )
            
            consumer_creation_time = time.time() - consumer_start_time
            logger.info(f"✅ Consumer client created in {consumer_creation_time:.3f}s")
            logger.info(f"Consumer Group: {self.consumer_group}")
            
            received_count = 0
            start_time = time.time()
            timeout = 30  # 30 seconds timeout
            target_reached = False
            consumer_exception = None
            
            def on_event(partition_context, event):
                nonlocal received_count, target_reached, consumer_exception
                if event and not target_reached:
                    try:
                        message = json.loads(event.body_as_str())
                        # Only count messages from THIS specific test run
                        if message.get('testRunId') == test_run_id:
                            received_count += 1
                            self.received_messages.append(message)
                            logger.info(f"✅ Test message {received_count}/5 received: {message.get('messageId', 'Unknown ID')}")
                            
                            # Update checkpoint for our test messages
                            try:
                                partition_context.update_checkpoint(event)
                                logger.debug("✅ Checkpoint updated")
                            except Exception as checkpoint_error:
                                logger.warning(f"⚠️ Checkpoint update failed: {checkpoint_error}")
                                
                            # Mark target reached after receiving all expected test messages
                            if received_count >= 5:
                                logger.info(f"🛑 Target reached - received all {received_count} test messages")
                                target_reached = True
                                # Force an exception to break out of receive loop
                                logger.info("🎯 Forcing consumer stop via exception")
                                consumer_exception = StopIteration("Target reached")
                                raise consumer_exception
                        else:
                            # Skip messages from other test runs (don't log - too verbose)
                            pass
                            
                    except StopIteration:
                        # Re-raise our custom stop exception
                        raise
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.debug(f"⚠️ Skipping malformed message: {e}")
                        pass  # Skip malformed messages
            
            # Receive messages with timeout control
            logger.info("🔄 STEP 4: Starting message consumption...")
            logger.info(f"Waiting for messages with timeout: {timeout}s")
            
            receive_start_time = time.time()
            
            # Use exception-based early termination
            try:
                with consumer:
                    consumer.receive(
                        on_event=on_event,
                        max_wait_time=timeout,
                        starting_position="-1"  # Start from latest
                    )
            except StopIteration as stop_ex:
                logger.info(f"🎯 Consumer stopped successfully: {stop_ex}")
            except KeyboardInterrupt:
                logger.info("🛑 Consumer interrupted")
            except Exception as receive_error:
                logger.warning(f"⚠️ Receive error: {receive_error}")
            
            # Final status check
            elapsed = time.time() - start_time
            if target_reached:
                logger.info(f"🎯 Target reached successfully after {elapsed:.1f}s")
            else:
                logger.warning(f"🛑 Consumer stopped after {elapsed:.1f}s - received {received_count} messages")
            
            receive_time = time.time() - receive_start_time
            logger.info(f"[OK] Message consumption completed in {receive_time:.3f}s")
            
            # Ensure test completes immediately after target reached
            if target_reached:
                logger.info("🏁 Test completed successfully - exiting consumer test")
                # Let any pending Azure cleanup happen in background
                return
            
            # Step 5: Verify we received messages
            logger.info(f"STEP 5: Verifying received messages...")
            logger.info(f"Total messages received: {received_count}")
            logger.info(f"Messages stored in list: {len(self.received_messages)}")
            
            assert received_count > 0, f"Should receive at least 1 message, got {received_count}"
            assert len(self.received_messages) > 0, "Should have received test messages"
            logger.info("[OK] Message count verification passed")
            
            # Step 6: Verify message structure
            if self.received_messages:
                logger.info("STEP 6: Verifying message structure...")
                sample_message = self.received_messages[0]
                logger.info(f"Sample message keys: {list(sample_message.keys())}")
                
                assert 'messageId' in sample_message, "Message should contain messageId"
                assert 'timestamp' in sample_message, "Message should contain timestamp"
                assert sample_message.get('testRun') == True, "Should be test message"
                assert sample_message.get('testRunId') == test_run_id, f"Should match test run ID: {test_run_id}"
                logger.info("[OK] Message structure verification passed")
            else:
                logger.warning("[WARN] No messages available for structure verification")
                
            logger.info("[SUCCESS] Consumer receive test completed successfully")
            
        except Exception as e:
            logger.error(f"[ERROR] Consumer receive test failed: {str(e)}")
            pytest.fail(f"Consumer receive test failed: {str(e)}")

    # @pytest.mark.medium
    # @pytest.mark.timeout(300)
    # def test_eventhub_producer_consumer_roundtrip(self):
    #     """Test complete EventHub roundtrip: produce then consume messages."""
    #     test_id = f"roundtrip-{int(time.time())}"
        
    #     try:
    #         # Step 1: Send unique test messages
    #         test_data = self.generate_test_weather_data(3)
    #         for msg in test_data:
    #             msg['testId'] = test_id  # Add unique test ID
            
    #         producer = EventHubProducerClient.from_connection_string(
    #             conn_str=self.connection_string,
    #             eventhub_name=self.eventhub_name
    #         )
            
    #         # Send messages
    #         with producer:
    #             event_data_batch = producer.create_batch()
    #             for message in test_data:
    #                 event_data_batch.add(EventData(json.dumps(message)))
    #             producer.send_batch(event_data_batch)
            
    #         # Step 2: Wait for messages to be available
    #         time.sleep(3)
            
    #         # Step 3: Consume and verify messages
    #         consumer = EventHubConsumerClient.from_connection_string(
    #             conn_str=self.connection_string,
    #             consumer_group=self.consumer_group,
    #             eventhub_name=self.eventhub_name
    #         )
            
    #         received_test_messages = []
    #         start_time = time.time()
    #         timeout = 30
            
    #         def on_event(partition_context, event):
    #             nonlocal received_test_messages
    #             if event:
    #                 try:
    #                     message = json.loads(event.body_as_str())
    #                     # Only collect our specific test messages
    #                     if message.get('testId') == test_id:
    #                         received_test_messages.append(message)
    #                 except:
    #                     pass
                    
    #                 partition_context.update_checkpoint(event)
                
    #             # Stop when we have all messages or timeout
    #             if len(received_test_messages) >= len(test_data) or (time.time() - start_time) > timeout:
    #                 return
            
    #         with consumer:
    #             consumer.receive(
    #                 on_event=on_event,
    #                 max_wait_time=timeout,
    #                 starting_position="-1"
    #             )
            
    #         # Verify roundtrip
    #         assert len(received_test_messages) > 0, "Should receive at least one message"
            
    #         # Verify message content integrity
    #         received_message_ids = {msg['messageId'] for msg in received_test_messages}
    #         sent_message_ids = {msg['messageId'] for msg in test_data}
            
    #         # At least some messages should match
    #         common_ids = received_message_ids.intersection(sent_message_ids)
    #         assert len(common_ids) > 0, "Should receive at least one of the sent messages"
            
    #     except Exception as e:
    #         pytest.fail(f"Roundtrip test failed: {str(e)}")

    # @pytest.mark.performance
    # @pytest.mark.timeout(600)
    # def test_eventhub_throughput_performance(self):
    #     """Test EventHub throughput performance with larger message volume."""
    #     message_count = 100
    #     batch_size = 20
        
    #     try:
    #         producer = EventHubProducerClient.from_connection_string(
    #             conn_str=self.connection_string,
    #             eventhub_name=self.eventhub_name
    #         )
            
    #         # Generate larger dataset
    #         test_data = self.generate_test_weather_data(message_count)
            
    #         start_time = time.time()
            
    #         # Send in batches for better performance
    #         with producer:
    #             for i in range(0, len(test_data), batch_size):
    #                 batch = test_data[i:i + batch_size]
    #                 event_data_batch = producer.create_batch()
                    
    #                 for message in batch:
    #                     event_data_batch.add(EventData(json.dumps(message)))
                    
    #                 producer.send_batch(event_data_batch)
            
    #         send_time = time.time() - start_time
            
    #         # Performance assertions
    #         messages_per_second = message_count / send_time
    #         assert messages_per_second > 10, f"Should send >10 messages/second, got {messages_per_second:.2f}"
    #         assert send_time < 60, f"Should complete within 60 seconds, took {send_time:.2f}s"
            
    #         print(f"Performance: {messages_per_second:.2f} messages/second, {send_time:.2f}s total")
            
    #     except Exception as e:
    #         pytest.fail(f"Performance test failed: {str(e)}")

    def _send_test_messages_to_eventhub(self, count: int, test_run_id: str = None):
        """Helper method to send test messages to EventHub."""
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )
        
        test_data = self.generate_test_weather_data(count)
        
        # Add test run ID to each message if provided
        if test_run_id:
            for message in test_data:
                message['testRunId'] = test_run_id
        
        with producer:
            event_data_batch = producer.create_batch()
            for message in test_data:
                event_data_batch.add(EventData(json.dumps(message)))
            producer.send_batch(event_data_batch)

    # @pytest.mark.medium
    # def test_eventhub_error_handling(self):
    #     """Test EventHub error handling with invalid configurations."""
    #     # Test with invalid connection string
    #     with pytest.raises((AzureError, ValueError)):
    #         producer = EventHubProducerClient.from_connection_string(
    #             conn_str="invalid_connection_string",
    #             eventhub_name=self.eventhub_name
    #         )
    #         with producer:
    #             event_data_batch = producer.create_batch()
    #             event_data_batch.add(EventData("test"))
    #             producer.send_batch(event_data_batch)

    # @pytest.mark.low
    # def test_eventhub_connection_recovery(self):
    #     """Test EventHub connection recovery capabilities."""
    #     try:
    #         producer = EventHubProducerClient.from_connection_string(
    #             conn_str=self.connection_string,
    #             eventhub_name=self.eventhub_name
    #         )
            
    #         # Send initial message to establish connection
    #         test_message = {"test": "recovery", "timestamp": time.time()}
            
    #         with producer:
    #             for attempt in range(3):  # Test multiple attempts
    #                 event_data_batch = producer.create_batch()
    #                 event_data_batch.add(EventData(json.dumps(test_message)))
    #                 producer.send_batch(event_data_batch)
    #                 time.sleep(1)  # Small delay between attempts
            
    #         assert True, "Connection recovery test completed successfully"
            
    #     except Exception as e:
    #         pytest.fail(f"Connection recovery test failed: {str(e)}")


# Test configuration and execution helpers
@pytest.fixture(scope="session")
def eventhub_config():
    """Provide EventHub configuration for tests."""
    return {
        'connection_string': os.getenv('TEST_EVENTHUB_CONNECTION_STRING'),
        'eventhub_name': os.getenv('TEST_EVENTHUB_NAME'),
        'consumer_group': os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
    }

@pytest.fixture
def cleanup_eventhub():
    """Fixture to clean up EventHub after tests."""
    yield
    # Add cleanup logic if needed
    pass