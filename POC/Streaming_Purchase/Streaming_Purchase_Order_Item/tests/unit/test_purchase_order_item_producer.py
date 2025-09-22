"""
Unit tests for PurchaseOrderItemProducer class.

Tests cover:
- EventHub message production
- Message serialization
- Batch processing
- Error handling and retries
- Connection management
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import json
import sys
import os

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from purchase_order_item_producer import PurchaseOrderItemProducer
from azure.eventhub import EventData, EventDataBatch


class TestPurchaseOrderItemProducer:
    """Test suite for PurchaseOrderItemProducer class."""

    @pytest.fixture
    def mock_producer_client(self):
        """Create a mock EventHub producer client."""
        with patch('purchase_order_item_producer.EventHubProducerClient') as mock:
            client = Mock()
            client.create_batch = Mock()
            client.send_batch = Mock()
            client.close = Mock()
            mock.from_connection_string.return_value = client
            yield client

    @pytest.fixture
    def producer(self, mock_producer_client):
        """Create a PurchaseOrderItemProducer instance for testing."""
        with patch('purchase_order_item_producer.EventHubProducerClient') as mock:
            mock.from_connection_string.return_value = mock_producer_client
            return PurchaseOrderItemProducer(
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
                eventhub_name="test-eventhub",
                batch_size=100,
                log_level="INFO"
            )

    def test_initialization(self, producer):
        """Test PurchaseOrderItemProducer initialization."""
        assert producer.eventhub_name == "test-eventhub"
        assert producer.batch_size == 100
        assert producer.logger is not None
        assert producer.producer_client is not None

    def test_send_single_message(self, producer, mock_producer_client):
        """Test sending a single message."""
        mock_batch = Mock()
        mock_batch.add = Mock(return_value=None)
        mock_producer_client.create_batch.return_value = mock_batch

        message = {
            "order_id": "ORD123",
            "item_id": "ITEM456",
            "quantity": 10,
            "timestamp": datetime.now().isoformat()
        }

        result = producer.send_message(message)

        assert result is True
        mock_batch.add.assert_called_once()
        mock_producer_client.send_batch.assert_called_once()

    def test_send_batch_messages(self, producer, mock_producer_client):
        """Test sending batch messages."""
        mock_batch = Mock()
        mock_batch.add = Mock(return_value=None)
        mock_producer_client.create_batch.return_value = mock_batch

        messages = [
            {"order_id": f"ORD{i}", "quantity": i}
            for i in range(10)
        ]

        result = producer.send_batch(messages)

        assert result is True
        assert mock_batch.add.call_count == 10
        mock_producer_client.send_batch.assert_called()

    def test_message_serialization(self, producer):
        """Test message serialization."""
        message = {
            "order_id": "ORD123",
            "timestamp": datetime.now(),
            "amount": 100.50,
            "items": ["item1", "item2"]
        }

        serialized = producer.serialize_message(message)

        assert isinstance(serialized, str)
        deserialized = json.loads(serialized)
        assert deserialized["order_id"] == "ORD123"
        assert deserialized["amount"] == 100.50

    def test_retry_logic(self, producer, mock_producer_client):
        """Test retry logic on failure."""
        mock_producer_client.send_batch.side_effect = [
            Exception("Connection error"),
            Exception("Timeout"),
            None  # Success on third attempt
        ]

        message = {"order_id": "ORD123"}
        result = producer.send_message_with_retry(message, max_retries=3)

        assert result is True
        assert mock_producer_client.send_batch.call_count == 3

    def test_connection_management(self, producer, mock_producer_client):
        """Test connection management."""
        # Test connection establishment
        producer.connect()
        assert producer.is_connected is True

        # Test connection closure
        producer.disconnect()
        mock_producer_client.close.assert_called_once()
        assert producer.is_connected is False

    def test_message_validation(self, producer):
        """Test message validation."""
        # Valid message
        valid_message = {
            "order_id": "ORD123",
            "item_id": "ITEM456",
            "quantity": 10
        }
        assert producer.validate_message(valid_message) is True

        # Invalid message (missing required field)
        invalid_message = {
            "quantity": 10
        }
        assert producer.validate_message(invalid_message) is False

    def test_batch_size_handling(self, producer, mock_producer_client):
        """Test handling of batch size limits."""
        mock_batch = Mock()
        mock_batch.add = Mock(side_effect=[None] * 100 + [ValueError("Batch full")])
        mock_producer_client.create_batch.return_value = mock_batch

        messages = [{"order_id": f"ORD{i}"} for i in range(150)]

        result = producer.send_batch(messages)

        assert result is True
        # Should have sent multiple batches
        assert mock_producer_client.send_batch.call_count >= 2

    def test_async_send(self, producer, mock_producer_client):
        """Test asynchronous message sending."""
        with patch('threading.Thread') as mock_thread:
            mock_thread_instance = Mock()
            mock_thread.return_value = mock_thread_instance

            message = {"order_id": "ORD123"}
            producer.send_async(message)

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

    def test_metrics_collection(self, producer):
        """Test metrics collection during sending."""
        producer.send_message({"order_id": "ORD123"})

        metrics = producer.get_metrics()

        assert metrics["messages_sent"] > 0
        assert metrics["batches_sent"] > 0
        assert "last_send_time" in metrics
        assert "errors" in metrics

    def test_error_handling(self, producer, mock_producer_client):
        """Test error handling for various scenarios."""
        # Test EventHub full error
        mock_producer_client.send_batch.side_effect = Exception("EventHub is full")

        with pytest.raises(Exception) as exc_info:
            producer.send_message({"order_id": "ORD123"}, raise_on_error=True)

        assert "EventHub is full" in str(exc_info.value)

    def test_partition_key_assignment(self, producer, mock_producer_client):
        """Test partition key assignment for messages."""
        mock_batch = Mock()
        mock_producer_client.create_batch.return_value = mock_batch

        message = {"order_id": "ORD123", "store_id": "STORE456"}
        producer.send_message(message, partition_key="STORE456")

        mock_producer_client.create_batch.assert_called_with(partition_key="STORE456")

    def test_cleanup_on_exit(self, producer, mock_producer_client):
        """Test cleanup when producer is destroyed."""
        del producer
        # Ensure close is called during cleanup
        mock_producer_client.close.assert_called()