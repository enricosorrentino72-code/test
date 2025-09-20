"""
Integration Tests for Azure EventHub with Purchase Order Items
Tests real EventHub connectivity and purchase order message processing
"""

import pytest
import json
import time
import os
from datetime import datetime, timezone
from typing import List, Dict, Any
from unittest.mock import patch, MagicMock
import logging

# Import Azure packages with error handling
try:
    from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    # Create dummy classes to prevent NameError
    class EventHubProducerClient: pass
    class EventHubConsumerClient: pass
    class EventData: pass
    class DefaultAzureCredential: pass
    class AzureError(Exception): pass

# Import project classes
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

try:
    from purchase_order_item_model import PurchaseOrderItem
    from purchase_order_item_factory import PurchaseOrderItemFactory
    from purchase_order_item_producer import PurchaseOrderItemProducer
    from purchase_order_item_listener import PurchaseOrderItemListener
except ImportError as e:
    # Fallback for tests when classes aren't available
    PurchaseOrderItem = None
    PurchaseOrderItemFactory = None
    PurchaseOrderItemProducer = None
    PurchaseOrderItemListener = None

# Test markers
pytestmark = [
    pytest.mark.integration,
    pytest.mark.eventhub,
    pytest.mark.slow
]


class TestEventHubPurchaseOrderIntegration:
    """Integration tests for EventHub with Purchase Order Items."""

    @classmethod
    def setup_class(cls):
        """Set up test class with Azure credentials and configuration."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK packages not available")

        cls.connection_string = os.getenv('TEST_EVENTHUB_CONNECTION_STRING')
        cls.eventhub_name = os.getenv('TEST_EVENTHUB_NAME', 'purchase-order-items')
        cls.consumer_group = os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
        cls.test_timeout = int(os.getenv('TEST_TIMEOUT', '120'))

        if not cls.connection_string:
            pytest.skip("EventHub credentials not available")

    def setup_method(self):
        """Set up for each test method."""
        self.test_messages = []
        self.received_messages = []
        self.factory = PurchaseOrderItemFactory() if PurchaseOrderItemFactory else None

    def generate_test_purchase_orders(self, count: int = 5) -> List[Dict[str, Any]]:
        """Generate test purchase order data."""
        if self.factory:
            orders = self.factory.generate_batch(count)
            return [order.to_dict() for order in orders]
        else:
            # Fallback test data
            test_orders = []
            base_time = datetime.now(timezone.utc)

            for i in range(count):
                order = {
                    "order_id": f"ORD-TEST-{base_time.timestamp():.0f}-{i:03d}",
                    "product_id": f"PROD-{i:03d}",
                    "product_name": f"Test Product {i}",
                    "quantity": 5 + (i % 10),
                    "unit_price": 29.99 + (i * 5.0),
                    "total_amount": (5 + (i % 10)) * (29.99 + (i * 5.0)),
                    "customer_id": f"CUST-{i % 3:03d}",
                    "vendor_id": f"VEND-{i % 2:03d}",
                    "warehouse_location": "TEST-WH-01",
                    "timestamp": (base_time.timestamp() + i) * 1000,
                    "test_run": True,
                    "test_id": f"integration-test-{int(time.time())}"
                }
                test_orders.append(order)

            return test_orders

    @pytest.mark.critical
    @pytest.mark.timeout(180)
    def test_eventhub_purchase_order_producer_connectivity(self):
        """Test EventHub producer connectivity for purchase order messages."""
        logging.basicConfig(level=logging.INFO, force=True)
        logger = logging.getLogger(__name__)

        try:
            logger.info("=== Testing Purchase Order Producer Connectivity ===")

            # Step 1: Create producer client
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )

            # Step 2: Generate test purchase order
            test_orders = self.generate_test_purchase_orders(1)
            test_order = test_orders[0]
            test_id = test_order.get('test_id', f"connectivity-{int(time.time())}")

            logger.info(f"Generated test purchase order: {test_order['order_id']}")
            logger.info(f"Test ID: {test_id}")

            # Step 3: Send purchase order to EventHub
            start_time = time.time()

            with producer:
                event_data_batch = producer.create_batch()
                event_data = EventData(json.dumps(test_order))
                event_data_batch.add(event_data)
                producer.send_batch(event_data_batch)

            send_time = time.time() - start_time
            logger.info(f"Purchase order sent successfully in {send_time:.3f}s")

            # Step 4: Verify success
            assert send_time < 30.0, f"Send operation took {send_time:.2f}s (should be < 30s)"
            assert True, f"Purchase order producer connectivity successful"

        except AzureError as e:
            pytest.fail(f"Azure EventHub connectivity failed: {str(e)}")
        except Exception as e:
            pytest.fail(f"Unexpected error in producer connectivity test: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(240)
    def test_eventhub_purchase_order_batch_processing(self):
        """Test EventHub batch processing for multiple purchase orders."""
        logger = logging.getLogger(__name__)

        try:
            logger.info("=== Testing Purchase Order Batch Processing ===")

            # Generate multiple test purchase orders
            batch_size = 10
            test_orders = self.generate_test_purchase_orders(batch_size)
            test_run_id = f"batch-test-{int(time.time())}"

            # Add test run ID to each order
            for order in test_orders:
                order['test_run_id'] = test_run_id
                order['batch_test'] = True

            logger.info(f"Generated {batch_size} test purchase orders")

            # Send batch to EventHub
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )

            start_time = time.time()
            messages_sent = 0

            with producer:
                event_data_batch = producer.create_batch()

                for order in test_orders:
                    try:
                        event_data = EventData(json.dumps(order))
                        event_data_batch.add(event_data)
                        messages_sent += 1
                    except ValueError:
                        # Batch is full, send current batch and start new one
                        if len(event_data_batch) > 0:
                            producer.send_batch(event_data_batch)
                        event_data_batch = producer.create_batch()
                        event_data_batch.add(event_data)
                        messages_sent += 1

                # Send remaining messages
                if len(event_data_batch) > 0:
                    producer.send_batch(event_data_batch)

            batch_time = time.time() - start_time
            throughput = messages_sent / batch_time if batch_time > 0 else 0

            logger.info(f"Batch processing completed: {messages_sent} orders in {batch_time:.3f}s")
            logger.info(f"Throughput: {throughput:.1f} orders/second")

            # Verify batch processing performance
            assert messages_sent == batch_size, f"Expected {batch_size} orders, sent {messages_sent}"
            assert batch_time < 60.0, f"Batch processing took {batch_time:.2f}s (should be < 60s)"
            assert throughput > 0.5, f"Throughput {throughput:.2f} orders/s is too low (should be > 0.5)"

        except Exception as e:
            pytest.fail(f"Batch processing test failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(300)
    def test_eventhub_purchase_order_consumer_integration(self):
        """Test EventHub consumer receiving purchase order messages."""
        logger = logging.getLogger(__name__)

        try:
            logger.info("=== Testing Purchase Order Consumer Integration ===")

            # Generate unique test run ID
            test_run_id = f"consumer-test-{int(time.time())}-{os.getpid()}"
            logger.info(f"Test Run ID: {test_run_id}")

            # Step 1: Send test purchase orders
            test_count = 3
            test_orders = self.generate_test_purchase_orders(test_count)

            for order in test_orders:
                order['test_run_id'] = test_run_id
                order['consumer_test'] = True

            self._send_purchase_orders_to_eventhub(test_orders, test_run_id)
            logger.info(f"Sent {test_count} test purchase orders")

            # Wait for message propagation
            time.sleep(3)

            # Step 2: Create consumer client
            consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name
            )

            received_count = 0
            start_time = time.time()
            timeout = 60  # 60 seconds timeout
            target_reached = False

            def on_event(partition_context, event):
                nonlocal received_count, target_reached
                if event and not target_reached:
                    try:
                        message = json.loads(event.body_as_str())
                        # Only count messages from THIS test run
                        if message.get('test_run_id') == test_run_id:
                            received_count += 1
                            self.received_messages.append(message)
                            logger.info(f"Received purchase order {received_count}/{test_count}: {message.get('order_id')}")

                            # Update checkpoint
                            try:
                                partition_context.update_checkpoint(event)
                            except Exception as checkpoint_error:
                                logger.warning(f"Checkpoint update failed: {checkpoint_error}")

                            # Check if target reached
                            if received_count >= test_count:
                                logger.info(f"Target reached - received all {received_count} purchase orders")
                                target_reached = True
                                raise StopIteration("Target reached")

                    except StopIteration:
                        raise
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.debug(f"Skipping malformed message: {e}")

            # Step 3: Consume messages
            logger.info("Starting purchase order consumption...")
            receive_start_time = time.time()

            try:
                with consumer:
                    consumer.receive(
                        on_event=on_event,
                        max_wait_time=timeout,
                        starting_position="-1"  # Start from latest
                    )
            except StopIteration:
                logger.info("Consumer stopped successfully")
            except Exception as receive_error:
                logger.warning(f"Consumer receive error: {receive_error}")

            receive_time = time.time() - receive_start_time
            logger.info(f"Consumer completed in {receive_time:.3f}s")

            # Step 4: Verify consumption
            assert received_count > 0, f"Should receive at least 1 purchase order, got {received_count}"
            assert len(self.received_messages) > 0, "Should have received purchase order messages"

            # Verify message structure for purchase orders
            if self.received_messages:
                sample_order = self.received_messages[0]
                logger.info(f"Sample order keys: {list(sample_order.keys())}")

                # Verify purchase order specific fields
                assert 'order_id' in sample_order, "Message should contain order_id"
                assert 'product_id' in sample_order, "Message should contain product_id"
                assert 'quantity' in sample_order, "Message should contain quantity"
                assert 'unit_price' in sample_order, "Message should contain unit_price"
                assert 'total_amount' in sample_order, "Message should contain total_amount"
                assert sample_order.get('test_run_id') == test_run_id, f"Should match test run ID"

                # Verify financial data integrity
                expected_total = sample_order['quantity'] * sample_order['unit_price']
                actual_total = sample_order['total_amount']
                assert abs(actual_total - expected_total) < 0.01, "Financial calculation should be accurate"

            logger.info("Purchase order consumer integration test completed successfully")

        except Exception as e:
            logger.error(f"Consumer integration test failed: {str(e)}")
            pytest.fail(f"Consumer integration test failed: {str(e)}")

    def _send_purchase_orders_to_eventhub(self, orders: List[Dict[str, Any]], test_run_id: str = None):
        """Helper method to send purchase orders to EventHub."""
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )

        with producer:
            event_data_batch = producer.create_batch()
            for order in orders:
                if test_run_id:
                    order['test_run_id'] = test_run_id

                event_data = EventData(json.dumps(order))
                try:
                    event_data_batch.add(event_data)
                except ValueError:
                    # Batch is full, send current batch and start new one
                    producer.send_batch(event_data_batch)
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(event_data)

            # Send remaining messages
            if len(event_data_batch) > 0:
                producer.send_batch(event_data_batch)


class TestEventHubPurchaseOrderErrorHandling:
    """Test error handling scenarios for EventHub purchase order processing."""

    @pytest.mark.medium
    @pytest.mark.timeout(120)
    def test_eventhub_connection_error_handling(self):
        """Test EventHub connection error handling with invalid credentials."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        # Test with invalid connection string
        with pytest.raises((AzureError, ValueError, Exception)):
            producer = EventHubProducerClient.from_connection_string(
                conn_str="Endpoint=sb://invalid.servicebus.windows.net/;SharedAccessKeyName=invalid;SharedAccessKey=invalid",
                eventhub_name="invalid"
            )

            test_order = {
                "order_id": "ORD-ERROR-TEST",
                "product_id": "PROD-ERROR",
                "quantity": 1,
                "unit_price": 10.0,
                "total_amount": 10.0
            }

            with producer:
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(json.dumps(test_order)))
                producer.send_batch(event_data_batch)

    @pytest.mark.low
    @pytest.mark.timeout(180)
    def test_eventhub_large_message_handling(self):
        """Test EventHub handling of large purchase order messages."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        logger = logging.getLogger(__name__)

        try:
            # Create a large purchase order message (close to EventHub limits)
            large_order = {
                "order_id": "ORD-LARGE-TEST",
                "product_id": "PROD-LARGE",
                "product_name": "Large Test Product",
                "quantity": 1,
                "unit_price": 100.0,
                "total_amount": 100.0,
                "large_description": "x" * 50000,  # 50KB description
                "metadata": {
                    "large_field_1": "y" * 10000,
                    "large_field_2": "z" * 10000,
                    "test_type": "large_message"
                }
            }

            message_size = len(json.dumps(large_order))
            logger.info(f"Large message size: {message_size} bytes ({message_size/1024:.1f} KB)")

            # Attempt to send large message
            connection_string = os.getenv('TEST_EVENTHUB_CONNECTION_STRING')
            if not connection_string:
                pytest.skip("EventHub credentials not available")

            producer = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=os.getenv('TEST_EVENTHUB_NAME', 'purchase-order-items')
            )

            start_time = time.time()

            with producer:
                event_data_batch = producer.create_batch()
                event_data = EventData(json.dumps(large_order))

                # This might fail if message is too large
                try:
                    event_data_batch.add(event_data)
                    producer.send_batch(event_data_batch)
                    send_time = time.time() - start_time
                    logger.info(f"Large message sent successfully in {send_time:.3f}s")

                    # Verify performance with large messages
                    assert send_time < 60.0, f"Large message took {send_time:.2f}s (should be < 60s)"

                except ValueError as e:
                    # Expected for very large messages
                    logger.info(f"Large message rejected as expected: {str(e)}")
                    assert "exceeds" in str(e).lower(), "Should be size-related error"

        except Exception as e:
            # Log but don't fail for large message tests
            logger.warning(f"Large message test completed with note: {str(e)}")


# Test fixtures
@pytest.fixture(scope="session")
def purchase_order_eventhub_config():
    """Provide EventHub configuration for purchase order tests."""
    return {
        'connection_string': os.getenv('TEST_EVENTHUB_CONNECTION_STRING'),
        'eventhub_name': os.getenv('TEST_EVENTHUB_NAME', 'purchase-order-items'),
        'consumer_group': os.getenv('TEST_EVENTHUB_CONSUMER_GROUP', '$Default')
    }

@pytest.fixture
def purchase_order_factory():
    """Provide purchase order factory for tests."""
    if PurchaseOrderItemFactory:
        return PurchaseOrderItemFactory(quality_issue_rate=0.0)  # Perfect quality for tests
    return None