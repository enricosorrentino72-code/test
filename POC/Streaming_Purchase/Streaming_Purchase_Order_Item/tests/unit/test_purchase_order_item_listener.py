"""
Unit tests for PurchaseOrderItemListener class.

Tests cover:
- EventHub message consumption
- Message deserialization
- Stream processing
- Error handling and recovery
- Checkpoint management
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call, AsyncMock
from datetime import datetime
import json
import sys
import os
import asyncio

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from purchase_order_item_listener import PurchaseOrderItemListener
from azure.eventhub import EventData, CheckpointStore


class TestPurchaseOrderItemListener:
    """Test suite for PurchaseOrderItemListener class."""

    @pytest.fixture
    def mock_consumer_client(self):
        """Create a mock EventHub consumer client."""
        with patch('purchase_order_item_listener.EventHubConsumerClient') as mock:
            client = Mock()
            client.receive = Mock()
            client.close = Mock()
            mock.from_connection_string.return_value = client
            yield client

    @pytest.fixture
    def listener(self, mock_consumer_client):
        """Create a PurchaseOrderItemListener instance for testing."""
        with patch('purchase_order_item_listener.EventHubConsumerClient') as mock:
            mock.from_connection_string.return_value = mock_consumer_client
            return PurchaseOrderItemListener(
                connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
                eventhub_name="test-eventhub",
                consumer_group="$Default",
                checkpoint_store=None,
                log_level="INFO"
            )

    def test_initialization(self, listener):
        """Test PurchaseOrderItemListener initialization."""
        assert listener.eventhub_name == "test-eventhub"
        assert listener.consumer_group == "$Default"
        assert listener.logger is not None
        assert listener.consumer_client is not None

    def test_start_listening(self, listener, mock_consumer_client):
        """Test starting the listener."""
        with patch.object(listener, 'process_event') as mock_process:
            listener.start_listening()

            mock_consumer_client.receive.assert_called_once()
            assert listener.is_listening is True

    def test_process_single_event(self, listener):
        """Test processing a single event."""
        mock_event = Mock()
        mock_event.body_as_str = Mock(return_value=json.dumps({
            "order_id": "ORD123",
            "item_id": "ITEM456",
            "quantity": 10
        }))
        mock_event.sequence_number = 100
        mock_event.offset = "1000"

        result = listener.process_event(mock_event)

        assert result is not None
        assert result["order_id"] == "ORD123"
        assert result["item_id"] == "ITEM456"

    def test_batch_processing(self, listener):
        """Test batch event processing."""
        events = []
        for i in range(10):
            mock_event = Mock()
            mock_event.body_as_str = Mock(return_value=json.dumps({
                "order_id": f"ORD{i}",
                "quantity": i
            }))
            events.append(mock_event)

        results = listener.process_batch(events)

        assert len(results) == 10
        assert results[0]["order_id"] == "ORD0"
        assert results[9]["order_id"] == "ORD9"

    def test_message_deserialization(self, listener):
        """Test message deserialization."""
        valid_json = '{"order_id": "ORD123", "amount": 100.50}'
        result = listener.deserialize_message(valid_json)

        assert result is not None
        assert result["order_id"] == "ORD123"
        assert result["amount"] == 100.50

        # Test invalid JSON
        invalid_json = '{"invalid": json}'
        result = listener.deserialize_message(invalid_json)
        assert result is None

    def test_checkpoint_management(self, listener):
        """Test checkpoint management."""
        with patch('purchase_order_item_listener.BlobCheckpointStore') as mock_checkpoint:
            checkpoint_store = Mock()
            listener.checkpoint_store = checkpoint_store

            mock_event = Mock()
            mock_event.partition = Mock(id="0")
            mock_event.offset = "1000"
            mock_event.sequence_number = 100

            listener.update_checkpoint(mock_event)

            checkpoint_store.update_checkpoint.assert_called_once()

    def test_error_handling(self, listener):
        """Test error handling during event processing."""
        mock_event = Mock()
        mock_event.body_as_str = Mock(side_effect=Exception("Decode error"))

        with pytest.raises(Exception) as exc_info:
            listener.process_event(mock_event, raise_on_error=True)

        assert "Decode error" in str(exc_info.value)

    def test_connection_recovery(self, listener, mock_consumer_client):
        """Test connection recovery after failure."""
        mock_consumer_client.receive.side_effect = [
            Exception("Connection lost"),
            None  # Success on retry
        ]

        listener.start_listening_with_retry(max_retries=2)

        assert mock_consumer_client.receive.call_count == 2
        assert listener.is_listening is True

    def test_message_filtering(self, listener):
        """Test message filtering."""
        messages = [
            {"order_id": "ORD1", "status": "NEW"},
            {"order_id": "ORD2", "status": "CANCELLED"},
            {"order_id": "ORD3", "status": "NEW"}
        ]

        filtered = listener.filter_messages(
            messages,
            filter_func=lambda m: m.get("status") == "NEW"
        )

        assert len(filtered) == 2
        assert all(m["status"] == "NEW" for m in filtered)

    def test_async_processing(self, listener):
        """Test asynchronous event processing."""
        async def mock_async_handler(event):
            await asyncio.sleep(0.1)
            return {"processed": True}

        mock_event = Mock()
        mock_event.body_as_str = Mock(return_value='{"order_id": "ORD123"}')

        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(
            listener.process_event_async(mock_event, mock_async_handler)
        )

        assert result["processed"] is True

    def test_metrics_tracking(self, listener):
        """Test metrics tracking during listening."""
        # Process some events
        for i in range(5):
            mock_event = Mock()
            mock_event.body_as_str = Mock(return_value=f'{{"order_id": "ORD{i}"}}')
            listener.process_event(mock_event)

        metrics = listener.get_metrics()

        assert metrics["events_processed"] == 5
        assert metrics["errors"] == 0
        assert "start_time" in metrics
        assert "last_event_time" in metrics

    def test_partition_assignment(self, listener, mock_consumer_client):
        """Test partition assignment for consumer."""
        listener.start_listening(starting_position="-1", partition_id="0")

        call_args = mock_consumer_client.receive.call_args
        assert call_args is not None

    def test_graceful_shutdown(self, listener, mock_consumer_client):
        """Test graceful shutdown."""
        listener.is_listening = True

        listener.stop_listening()

        assert listener.is_listening is False
        mock_consumer_client.close.assert_called_once()

    def test_dead_letter_handling(self, listener):
        """Test dead letter queue handling."""
        mock_event = Mock()
        mock_event.body_as_str = Mock(return_value='invalid json')

        result = listener.send_to_dead_letter(mock_event, "Parse error")

        assert result is True
        assert listener.dead_letter_count == 1

    def test_custom_handler_registration(self, listener):
        """Test registering custom event handlers."""
        def custom_handler(event_data):
            return {"custom": "processed"}

        listener.register_handler("custom", custom_handler)

        assert "custom" in listener.handlers
        assert listener.handlers["custom"] == custom_handler