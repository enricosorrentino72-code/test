"""
Azure service mocks for testing.

Provides mock implementations for:
- Azure EventHub
- Azure Data Lake Storage (ADLS) Gen2
- Azure Service Bus
- Azure Key Vault
"""

from unittest.mock import Mock, MagicMock, AsyncMock
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import uuid


class MockEventHubProducerClient:
    """Mock implementation of Azure EventHub Producer Client."""

    def __init__(self, connection_string: str = None, eventhub_name: str = None):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.is_closed = False
        self.sent_messages = []
        self.sent_batches = []

    def create_batch(self, partition_key: str = None, partition_id: str = None):
        """Create a mock event batch."""
        return MockEventDataBatch(partition_key=partition_key, partition_id=partition_id)

    def send_batch(self, event_data_batch):
        """Send a batch of events."""
        if self.is_closed:
            raise Exception("Producer client is closed")

        self.sent_batches.append({
            "batch": event_data_batch,
            "timestamp": datetime.now(),
            "message_count": len(event_data_batch.events)
        })

        # Add to sent messages for tracking
        self.sent_messages.extend(event_data_batch.events)

    def send_event(self, event_data):
        """Send a single event."""
        batch = self.create_batch()
        batch.add(event_data)
        self.send_batch(batch)

    def close(self):
        """Close the producer client."""
        self.is_closed = True

    @classmethod
    def from_connection_string(cls, connection_string: str, eventhub_name: str, **kwargs):
        """Create producer from connection string."""
        return cls(connection_string=connection_string, eventhub_name=eventhub_name)

    def get_sent_messages(self) -> List[Dict]:
        """Get all sent messages for testing verification."""
        return [msg.body for msg in self.sent_messages]


class MockEventHubConsumerClient:
    """Mock implementation of Azure EventHub Consumer Client."""

    def __init__(self, connection_string: str = None, consumer_group: str = None, eventhub_name: str = None):
        self.connection_string = connection_string
        self.consumer_group = consumer_group
        self.eventhub_name = eventhub_name
        self.is_closed = False
        self.mock_events = []
        self.checkpoints = {}

    def receive(self, on_event=None, on_partition_initialize=None, on_partition_close=None,
                on_error=None, max_batch_size=300, max_wait_time=None, partition_id=None,
                starting_position=None, starting_position_inclusive=None):
        """Receive events from EventHub."""
        if self.is_closed:
            raise Exception("Consumer client is closed")

        # Return mock events
        return self.mock_events

    def receive_batch(self, partition_id: str, max_batch_size: int = 300, max_wait_time: float = None):
        """Receive a batch of events."""
        return self.mock_events[:max_batch_size]

    def close(self):
        """Close the consumer client."""
        self.is_closed = True

    @classmethod
    def from_connection_string(cls, connection_string: str, consumer_group: str, eventhub_name: str, **kwargs):
        """Create consumer from connection string."""
        return cls(connection_string=connection_string, consumer_group=consumer_group, eventhub_name=eventhub_name)

    def add_mock_event(self, event_data: Dict, partition_id: str = "0", sequence_number: int = None):
        """Add a mock event for testing."""
        mock_event = MockEventData(
            body=json.dumps(event_data) if isinstance(event_data, dict) else event_data,
            partition_id=partition_id,
            sequence_number=sequence_number or len(self.mock_events) + 1
        )
        self.mock_events.append(mock_event)


class MockEventData:
    """Mock implementation of Azure EventData."""

    def __init__(self, body: str, partition_id: str = "0", sequence_number: int = 1, offset: str = "1000"):
        self.body = body
        self.partition_id = partition_id
        self.sequence_number = sequence_number
        self.offset = offset
        self.enqueued_time = datetime.now()
        self.partition = Mock(id=partition_id)

    def body_as_str(self, encoding: str = 'UTF-8') -> str:
        """Get body as string."""
        if isinstance(self.body, bytes):
            return self.body.decode(encoding)
        return str(self.body)

    def body_as_json(self) -> Dict:
        """Get body as JSON."""
        return json.loads(self.body_as_str())


class MockEventDataBatch:
    """Mock implementation of Azure EventDataBatch."""

    def __init__(self, partition_key: str = None, partition_id: str = None, max_size: int = 1000000):
        self.partition_key = partition_key
        self.partition_id = partition_id
        self.max_size = max_size
        self.events = []
        self.size = 0

    def add(self, event_data):
        """Add event to batch."""
        if isinstance(event_data, dict):
            event_body = json.dumps(event_data)
        elif isinstance(event_data, str):
            event_body = event_data
        else:
            event_body = str(event_data.body) if hasattr(event_data, 'body') else str(event_data)

        event_size = len(event_body.encode('utf-8'))

        if self.size + event_size > self.max_size:
            raise ValueError("Batch size would exceed maximum")

        mock_event = Mock()
        mock_event.body = event_body
        self.events.append(mock_event)
        self.size += event_size

    def __len__(self):
        return len(self.events)


class MockDataLakeServiceClient:
    """Mock implementation of Azure Data Lake Service Client."""

    def __init__(self, account_url: str = None, credential: str = None):
        self.account_url = account_url
        self.credential = credential
        self.file_systems = {}

    def get_file_system_client(self, file_system: str):
        """Get a file system client."""
        if file_system not in self.file_systems:
            self.file_systems[file_system] = MockFileSystemClient(file_system)
        return self.file_systems[file_system]

    @classmethod
    def from_connection_string(cls, connection_string: str, **kwargs):
        """Create client from connection string."""
        return cls(account_url="https://test.dfs.core.windows.net", credential="test_credential")


class MockFileSystemClient:
    """Mock implementation of Azure File System Client."""

    def __init__(self, name: str):
        self.name = name
        self.directories = {}
        self.files = {}

    def exists(self) -> bool:
        """Check if file system exists."""
        return True

    def create_directory(self, directory: str):
        """Create a directory."""
        self.directories[directory] = MockDirectoryClient(directory)
        return self.directories[directory]

    def get_directory_client(self, directory: str):
        """Get a directory client."""
        if directory not in self.directories:
            self.directories[directory] = MockDirectoryClient(directory)
        return self.directories[directory]

    def get_file_client(self, file_path: str):
        """Get a file client."""
        if file_path not in self.files:
            self.files[file_path] = MockFileClient(file_path)
        return self.files[file_path]

    def get_paths(self, path: str = None, recursive: bool = False):
        """List paths in the file system."""
        paths = []

        # Add directories
        for dir_path in self.directories.keys():
            if not path or dir_path.startswith(path):
                mock_path = Mock()
                mock_path.name = dir_path.split('/')[-1]
                mock_path.is_directory = True
                paths.append(mock_path)

        # Add files
        for file_path in self.files.keys():
            if not path or file_path.startswith(path):
                mock_path = Mock()
                mock_path.name = file_path.split('/')[-1]
                mock_path.is_directory = False
                paths.append(mock_path)

        return paths

    def delete_directory(self, directory: str):
        """Delete a directory."""
        if directory in self.directories:
            del self.directories[directory]


class MockDirectoryClient:
    """Mock implementation of Azure Directory Client."""

    def __init__(self, path: str):
        self.path = path
        self.properties = {
            "last_modified": datetime.now(),
            "creation_time": datetime.now(),
            "content_length": 0
        }

    def create_directory(self):
        """Create the directory."""
        return self

    def delete_directory(self):
        """Delete the directory."""
        pass

    def get_directory_properties(self):
        """Get directory properties."""
        return self.properties

    def set_access_control(self, acl: str = None, **kwargs):
        """Set access control."""
        pass

    def get_access_control(self):
        """Get access control."""
        return {"acl": "user::rwx,group::r-x,other::r--"}


class MockFileClient:
    """Mock implementation of Azure File Client."""

    def __init__(self, path: str):
        self.path = path
        self.content = b""
        self.properties = {
            "last_modified": datetime.now(),
            "creation_time": datetime.now(),
            "content_length": 0
        }

    def upload_data(self, data, overwrite: bool = False, **kwargs):
        """Upload data to file."""
        if isinstance(data, str):
            self.content = data.encode('utf-8')
        else:
            self.content = data
        self.properties["content_length"] = len(self.content)

    def download_file(self):
        """Download file content."""
        mock_download = Mock()
        mock_download.readall = Mock(return_value=self.content)
        return mock_download

    def delete_file(self):
        """Delete the file."""
        self.content = b""

    def get_file_properties(self):
        """Get file properties."""
        return self.properties


class MockBlobCheckpointStore:
    """Mock implementation of Azure Blob Checkpoint Store."""

    def __init__(self, blob_account_url: str = None, container_name: str = None, credential: str = None):
        self.blob_account_url = blob_account_url
        self.container_name = container_name
        self.credential = credential
        self.checkpoints = {}

    def update_checkpoint(self, fully_qualified_namespace: str, eventhub_name: str,
                         consumer_group: str, partition_id: str, **kwargs):
        """Update checkpoint."""
        key = f"{fully_qualified_namespace}:{eventhub_name}:{consumer_group}:{partition_id}"
        self.checkpoints[key] = {
            "sequence_number": kwargs.get("sequence_number"),
            "offset": kwargs.get("offset"),
            "timestamp": datetime.now()
        }

    def list_checkpoints(self, fully_qualified_namespace: str, eventhub_name: str, consumer_group: str):
        """List checkpoints."""
        prefix = f"{fully_qualified_namespace}:{eventhub_name}:{consumer_group}:"
        return [v for k, v in self.checkpoints.items() if k.startswith(prefix)]


class MockServiceBusClient:
    """Mock implementation of Azure Service Bus Client."""

    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string
        self.messages = []

    def get_queue_sender(self, queue_name: str):
        """Get queue sender."""
        return MockServiceBusSender(queue_name)

    def get_queue_receiver(self, queue_name: str):
        """Get queue receiver."""
        return MockServiceBusReceiver(queue_name)


class MockServiceBusSender:
    """Mock implementation of Service Bus Sender."""

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.sent_messages = []

    def send_messages(self, messages):
        """Send messages to queue."""
        if isinstance(messages, list):
            self.sent_messages.extend(messages)
        else:
            self.sent_messages.append(messages)

    def close(self):
        """Close sender."""
        pass


class MockServiceBusReceiver:
    """Mock implementation of Service Bus Receiver."""

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.mock_messages = []

    def receive_messages(self, max_message_count: int = 1, max_wait_time: int = None):
        """Receive messages from queue."""
        return self.mock_messages[:max_message_count]

    def add_mock_message(self, body: str):
        """Add mock message for testing."""
        mock_message = Mock()
        mock_message.body = body
        mock_message.message_id = str(uuid.uuid4())
        self.mock_messages.append(mock_message)

    def close(self):
        """Close receiver."""
        pass


def create_mock_azure_environment():
    """Create a complete mock Azure environment for testing."""
    return {
        "eventhub_producer": MockEventHubProducerClient(),
        "eventhub_consumer": MockEventHubConsumerClient(),
        "adls_client": MockDataLakeServiceClient(),
        "checkpoint_store": MockBlobCheckpointStore(),
        "service_bus": MockServiceBusClient()
    }