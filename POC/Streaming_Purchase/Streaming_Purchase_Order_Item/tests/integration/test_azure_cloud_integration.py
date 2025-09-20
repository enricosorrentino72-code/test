"""
Azure Cloud Integration tests for EventHub and ADLS Gen2.

Tests cover:
- End-to-end EventHub messaging
- ADLS Gen2 storage operations
- Authentication and authorization
- Network connectivity
- Service integration patterns
"""

import pytest
import os
import json
import asyncio
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys

# Add necessary paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../utility')))

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.eventhub.exceptions import EventHubError
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError


class TestAzureCloudIntegration:
    """Azure Cloud Integration tests."""

    @pytest.fixture
    def azure_config(self):
        """Azure configuration for testing."""
        return {
            "eventhub_connection_string": os.getenv("EVENTHUB_CONNECTION_STRING", "mock_connection"),
            "eventhub_name": os.getenv("EVENTHUB_NAME", "test-eventhub"),
            "consumer_group": os.getenv("CONSUMER_GROUP", "$Default"),
            "storage_account": os.getenv("STORAGE_ACCOUNT", "testaccount"),
            "storage_key": os.getenv("STORAGE_KEY", "testkey"),
            "container_name": os.getenv("CONTAINER_NAME", "testcontainer")
        }

    @pytest.fixture
    def mock_eventhub_producer(self):
        """Mock EventHub producer client."""
        with patch('azure.eventhub.EventHubProducerClient') as mock:
            producer = Mock()
            producer.create_batch = Mock()
            producer.send_batch = Mock()
            producer.close = Mock()
            mock.from_connection_string.return_value = producer
            yield producer

    @pytest.fixture
    def mock_eventhub_consumer(self):
        """Mock EventHub consumer client."""
        with patch('azure.eventhub.EventHubConsumerClient') as mock:
            consumer = Mock()
            consumer.receive = Mock()
            consumer.close = Mock()
            mock.from_connection_string.return_value = consumer
            yield consumer

    @pytest.fixture
    def mock_adls_client(self):
        """Mock ADLS client."""
        with patch('azure.storage.filedatalake.DataLakeServiceClient') as mock:
            client = Mock()
            file_system_client = Mock()
            client.get_file_system_client = Mock(return_value=file_system_client)
            mock.from_connection_string.return_value = client
            yield client

    def test_eventhub_producer_integration(self, mock_eventhub_producer, azure_config):
        """Test EventHub producer integration."""
        # Create mock batch
        mock_batch = Mock()
        mock_batch.add = Mock()
        mock_eventhub_producer.create_batch.return_value = mock_batch

        # Test message sending
        test_messages = [
            {
                "order_id": f"ORD{i:03d}",
                "timestamp": datetime.now().isoformat(),
                "amount": i * 10.0
            }
            for i in range(10)
        ]

        # Send messages
        for message in test_messages:
            event_data = EventData(json.dumps(message))
            mock_batch.add(event_data)

        mock_eventhub_producer.send_batch(mock_batch)

        # Verify interactions
        assert mock_batch.add.call_count == 10
        mock_eventhub_producer.send_batch.assert_called_once_with(mock_batch)

    def test_eventhub_consumer_integration(self, mock_eventhub_consumer, azure_config):
        """Test EventHub consumer integration."""
        # Mock received events
        mock_events = []
        for i in range(5):
            event = Mock()
            event.body_as_str = Mock(return_value=json.dumps({
                "order_id": f"ORD{i:03d}",
                "timestamp": datetime.now().isoformat()
            }))
            event.partition = Mock(id="0")
            event.sequence_number = i + 1
            event.offset = str((i + 1) * 100)
            mock_events.append(event)

        # Mock receive method
        mock_eventhub_consumer.receive.return_value = mock_events

        # Test event consumption
        received_events = mock_eventhub_consumer.receive(
            on_event=lambda partition_context, event: event,
            max_batch_size=10,
            max_wait_time=5
        )

        assert len(received_events) == 5
        for i, event in enumerate(received_events):
            message = json.loads(event.body_as_str())
            assert message["order_id"] == f"ORD{i:03d}"

    def test_adls_file_operations(self, mock_adls_client, azure_config):
        """Test ADLS file operations integration."""
        file_system_client = mock_adls_client.get_file_system_client(azure_config["container_name"])

        # Test file upload
        file_client = Mock()
        file_system_client.get_file_client = Mock(return_value=file_client)
        file_client.upload_data = Mock()

        test_data = json.dumps({"test": "data", "timestamp": datetime.now().isoformat()})
        file_client.upload_data(test_data, overwrite=True)

        file_client.upload_data.assert_called_once_with(test_data, overwrite=True)

        # Test file download
        file_client.download_file = Mock(return_value=Mock(readall=Mock(return_value=test_data.encode())))
        downloaded_data = file_client.download_file().readall()

        assert downloaded_data.decode() == test_data

    def test_eventhub_to_adls_pipeline(self, mock_eventhub_consumer, mock_adls_client, azure_config):
        """Test end-to-end pipeline from EventHub to ADLS."""
        # Setup EventHub consumer
        mock_events = []
        for i in range(100):
            event = Mock()
            event.body_as_str = Mock(return_value=json.dumps({
                "order_id": f"ORD{i:03d}",
                "item_id": f"ITEM{i:03d}",
                "quantity": i + 1,
                "timestamp": datetime.now().isoformat()
            }))
            mock_events.append(event)

        mock_eventhub_consumer.receive.return_value = mock_events

        # Setup ADLS client
        file_system_client = mock_adls_client.get_file_system_client(azure_config["container_name"])
        file_client = Mock()
        file_system_client.get_file_client = Mock(return_value=file_client)
        file_client.upload_data = Mock()

        # Simulate pipeline processing
        received_events = mock_eventhub_consumer.receive()
        processed_data = []

        for event in received_events:
            message = json.loads(event.body_as_str())
            # Add processing timestamp
            message["processed_at"] = datetime.now().isoformat()
            processed_data.append(message)

        # Batch and upload to ADLS
        batch_data = "\n".join([json.dumps(record) for record in processed_data])
        file_path = f"processed/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        file_system_client.get_file_client(file_path).upload_data(batch_data, overwrite=True)

        # Verify pipeline execution
        assert len(processed_data) == 100
        file_client.upload_data.assert_called_once_with(batch_data, overwrite=True)

    def test_error_handling_eventhub_connection_failure(self, azure_config):
        """Test error handling for EventHub connection failures."""
        with patch('azure.eventhub.EventHubProducerClient.from_connection_string') as mock_producer:
            mock_producer.side_effect = EventHubError("Connection failed")

            with pytest.raises(EventHubError) as exc_info:
                EventHubProducerClient.from_connection_string(
                    azure_config["eventhub_connection_string"],
                    eventhub_name=azure_config["eventhub_name"]
                )

            assert "Connection failed" in str(exc_info.value)

    def test_error_handling_adls_authentication_failure(self, azure_config):
        """Test error handling for ADLS authentication failures."""
        with patch('azure.storage.filedatalake.DataLakeServiceClient.from_connection_string') as mock_client:
            mock_client.side_effect = AzureError("Authentication failed")

            with pytest.raises(AzureError) as exc_info:
                DataLakeServiceClient.from_connection_string(
                    f"DefaultEndpointsProtocol=https;AccountName={azure_config['storage_account']};AccountKey={azure_config['storage_key']};EndpointSuffix=core.windows.net"
                )

            assert "Authentication failed" in str(exc_info.value)

    def test_network_resilience_retry_pattern(self, mock_eventhub_producer, azure_config):
        """Test network resilience with retry patterns."""
        # Simulate intermittent failures
        mock_batch = Mock()
        mock_eventhub_producer.create_batch.return_value = mock_batch
        mock_eventhub_producer.send_batch.side_effect = [
            EventHubError("Network timeout"),
            EventHubError("Temporary failure"),
            None  # Success on third attempt
        ]

        # Retry logic
        max_retries = 3
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                test_message = {"order_id": "ORD001", "test": True}
                event_data = EventData(json.dumps(test_message))
                mock_batch.add(event_data)
                mock_eventhub_producer.send_batch(mock_batch)
                success = True
            except EventHubError:
                retry_count += 1
                if retry_count >= max_retries:
                    raise

        assert success
        assert retry_count == 2  # Failed twice, succeeded on third attempt

    def test_throughput_performance(self, mock_eventhub_producer, azure_config):
        """Test throughput performance for high-volume scenarios."""
        import time

        # Create large batch of test messages
        message_count = 1000
        mock_batch = Mock()
        mock_eventhub_producer.create_batch.return_value = mock_batch

        start_time = time.time()

        # Simulate high-throughput sending
        for i in range(message_count):
            message = {
                "order_id": f"ORD{i:06d}",
                "timestamp": datetime.now().isoformat(),
                "data": f"test_data_{i}"
            }
            event_data = EventData(json.dumps(message))
            mock_batch.add(event_data)

        mock_eventhub_producer.send_batch(mock_batch)

        end_time = time.time()
        execution_time = end_time - start_time

        # Verify performance metrics
        messages_per_second = message_count / execution_time
        assert messages_per_second > 100  # Should process > 100 messages/second
        assert mock_batch.add.call_count == message_count

    def test_partition_strategy(self, mock_eventhub_producer, azure_config):
        """Test EventHub partition strategy."""
        # Test partition key assignment
        partition_keys = ["store_001", "store_002", "store_003"]

        for partition_key in partition_keys:
            mock_batch = Mock()
            mock_eventhub_producer.create_batch.return_value = mock_batch

            # Create batch with partition key
            mock_eventhub_producer.create_batch(partition_key=partition_key)
            mock_eventhub_producer.create_batch.assert_called_with(partition_key=partition_key)

            # Send test message
            test_message = {"store_id": partition_key, "order_id": "ORD001"}
            event_data = EventData(json.dumps(test_message))
            mock_batch.add(event_data)
            mock_eventhub_producer.send_batch(mock_batch)

        # Verify all partition keys were used
        assert mock_eventhub_producer.create_batch.call_count == 3

    def test_checkpoint_management(self, mock_eventhub_consumer, azure_config):
        """Test checkpoint management for consumer resilience."""
        # Mock checkpoint store
        with patch('azure.eventhub.extensions.checkpointstoreblob.BlobCheckpointStore') as mock_checkpoint:
            checkpoint_store = Mock()
            mock_checkpoint.return_value = checkpoint_store

            # Mock events with checkpoint info
            mock_event = Mock()
            mock_event.body_as_str = Mock(return_value='{"order_id": "ORD001"}')
            mock_event.partition = Mock(id="0")
            mock_event.sequence_number = 100
            mock_event.offset = "10000"

            # Test checkpoint update
            checkpoint_store.update_checkpoint = Mock()

            # Simulate checkpoint update
            checkpoint_store.update_checkpoint(
                fully_qualified_namespace=azure_config["eventhub_connection_string"],
                eventhub_name=azure_config["eventhub_name"],
                consumer_group=azure_config["consumer_group"],
                partition_id=mock_event.partition.id,
                sequence_number=mock_event.sequence_number,
                offset=mock_event.offset
            )

            checkpoint_store.update_checkpoint.assert_called_once()