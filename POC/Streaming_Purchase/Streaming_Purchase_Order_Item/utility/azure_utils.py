# Azure Utilities for Purchase Order Item Pipeline

import json
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import time

# Azure EventHub imports
try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.eventhub.aio import EventHubConsumerClient
    from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logging.warning("Azure EventHub libraries not available - using mock implementation")

logger = logging.getLogger(__name__)

class AzureEventHubUtils:
    """
    Utility class for Azure EventHub operations specific to Purchase Order Item pipeline.

    Provides functionality for sending and receiving purchase order events,
    with proper error handling and retry logic.
    """

    def __init__(self,
                 connection_string: str,
                 eventhub_name: str,
                 consumer_group: str = "$Default"):
        """
        Initialize Azure EventHub utilities.

        Args:
            connection_string: EventHub connection string
            eventhub_name: Name of the EventHub
            consumer_group: Consumer group name
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.producer_client = None
        self.consumer_client = None

        if not AZURE_AVAILABLE:
            logger.warning("Azure EventHub libraries not available - using mock implementation")

    def create_producer_client(self) -> Optional[EventHubProducerClient]:
        """
        Create EventHub producer client.

        Returns:
            EventHubProducerClient: Producer client or None if creation fails
        """
        if not AZURE_AVAILABLE:
            logger.warning("Azure EventHub not available - returning mock producer")
            return MockEventHubProducer()

        try:
            self.producer_client = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            logger.info("EventHub producer client created successfully")
            return self.producer_client

        except Exception as e:
            logger.error(f"Error creating EventHub producer client: {str(e)}")
            return None

    def create_consumer_client(self,
                              checkpoint_store: Optional[BlobCheckpointStore] = None) -> Optional[EventHubConsumerClient]:
        """
        Create EventHub consumer client.

        Args:
            checkpoint_store: Optional blob checkpoint store

        Returns:
            EventHubConsumerClient: Consumer client or None if creation fails
        """
        if not AZURE_AVAILABLE:
            logger.warning("Azure EventHub not available - returning mock consumer")
            return MockEventHubConsumer()

        try:
            self.consumer_client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name,
                checkpoint_store=checkpoint_store
            )
            logger.info("EventHub consumer client created successfully")
            return self.consumer_client

        except Exception as e:
            logger.error(f"Error creating EventHub consumer client: {str(e)}")
            return None

    def send_purchase_order_batch(self,
                                 purchase_orders: List[Dict[str, Any]],
                                 partition_key: Optional[str] = None,
                                 max_retries: int = 3) -> Tuple[bool, Dict[str, Any]]:
        """
        Send a batch of purchase order items to EventHub.

        Args:
            purchase_orders: List of purchase order item dictionaries
            partition_key: Optional partition key for routing
            max_retries: Maximum number of retry attempts

        Returns:
            Tuple[bool, Dict]: Success status and send statistics
        """
        if not self.producer_client:
            self.producer_client = self.create_producer_client()

        if not self.producer_client:
            return False, {"error": "Failed to create producer client"}

        start_time = time.time()
        stats = {
            "total_events": len(purchase_orders),
            "sent_events": 0,
            "failed_events": 0,
            "batches_sent": 0,
            "retry_attempts": 0,
            "send_duration_seconds": 0,
            "errors": []
        }

        for attempt in range(max_retries + 1):
            try:
                # Create event batch
                event_batch = self.producer_client.create_batch(partition_key=partition_key)

                batch_count = 0
                failed_in_batch = 0

                for order in purchase_orders:
                    try:
                        # Convert to JSON and create event
                        event_data = EventData(json.dumps(order, default=str))

                        # Add custom properties
                        event_data.properties = {
                            "content-type": "application/json",
                            "event-type": "purchase-order-item",
                            "source": "purchase-order-pipeline",
                            "timestamp": datetime.utcnow().isoformat()
                        }

                        # Try to add to batch
                        event_batch.add(event_data)
                        batch_count += 1

                    except ValueError as e:
                        # Batch is full, send current batch and create new one
                        if "EventDataBatch object is full" in str(e):
                            if batch_count > 0:
                                self.producer_client.send_batch(event_batch)
                                stats["batches_sent"] += 1
                                stats["sent_events"] += batch_count

                            # Create new batch and add current event
                            event_batch = self.producer_client.create_batch(partition_key=partition_key)
                            event_batch.add(EventData(json.dumps(order, default=str)))
                            batch_count = 1
                        else:
                            logger.error(f"Error adding event to batch: {str(e)}")
                            failed_in_batch += 1
                            stats["errors"].append(str(e))

                    except Exception as e:
                        logger.error(f"Error processing purchase order event: {str(e)}")
                        failed_in_batch += 1
                        stats["errors"].append(str(e))

                # Send final batch if it has events
                if batch_count > 0:
                    self.producer_client.send_batch(event_batch)
                    stats["batches_sent"] += 1
                    stats["sent_events"] += batch_count

                stats["failed_events"] = failed_in_batch
                stats["send_duration_seconds"] = time.time() - start_time

                logger.info(f"Successfully sent {stats['sent_events']} purchase order events "
                           f"in {stats['batches_sent']} batches")

                return True, stats

            except Exception as e:
                stats["retry_attempts"] = attempt + 1
                error_msg = f"Attempt {attempt + 1} failed: {str(e)}"
                stats["errors"].append(error_msg)
                logger.error(error_msg)

                if attempt < max_retries:
                    # Exponential backoff
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} attempts failed")

        stats["send_duration_seconds"] = time.time() - start_time
        return False, stats

    def send_single_purchase_order(self,
                                  purchase_order: Dict[str, Any],
                                  partition_key: Optional[str] = None) -> bool:
        """
        Send a single purchase order item to EventHub.

        Args:
            purchase_order: Purchase order item dictionary
            partition_key: Optional partition key

        Returns:
            bool: Success status
        """
        success, stats = self.send_purchase_order_batch([purchase_order], partition_key)
        return success and stats.get("sent_events", 0) == 1

    async def consume_purchase_orders(self,
                                    on_event_callback,
                                    on_error_callback=None,
                                    starting_position: str = "-1",
                                    max_events: Optional[int] = None) -> None:
        """
        Consume purchase order events from EventHub.

        Args:
            on_event_callback: Callback function for processing events
            on_error_callback: Optional callback for error handling
            starting_position: Starting position for consumption
            max_events: Maximum number of events to consume
        """
        if not self.consumer_client:
            self.consumer_client = self.create_consumer_client()

        if not self.consumer_client:
            logger.error("Failed to create consumer client")
            return

        events_consumed = 0

        try:
            async with self.consumer_client:
                await self.consumer_client.receive(
                    on_event=lambda partition_context, event: self._handle_event(
                        partition_context, event, on_event_callback, on_error_callback
                    ),
                    on_error=on_error_callback or self._default_error_handler,
                    starting_position=starting_position
                )

        except Exception as e:
            logger.error(f"Error consuming events: {str(e)}")
            if on_error_callback:
                await on_error_callback(e)

    async def _handle_event(self, partition_context, event, on_event_callback, on_error_callback):
        """
        Handle individual event from EventHub.
        """
        try:
            # Parse event data
            event_data = json.loads(event.body_as_str())

            # Add event metadata
            event_metadata = {
                "partition_id": partition_context.partition_id,
                "sequence_number": event.sequence_number,
                "offset": event.offset,
                "enqueued_time": event.enqueued_time,
                "properties": event.properties
            }

            # Call user callback
            await on_event_callback(event_data, event_metadata)

            # Update checkpoint
            await partition_context.update_checkpoint(event)

        except Exception as e:
            logger.error(f"Error handling event: {str(e)}")
            if on_error_callback:
                await on_error_callback(e)

    async def _default_error_handler(self, error):
        """
        Default error handler for EventHub operations.
        """
        logger.error(f"EventHub error: {str(error)}")

    def close_connections(self):
        """
        Close EventHub client connections.
        """
        try:
            if self.producer_client:
                self.producer_client.close()
                logger.info("Producer client closed")

            if self.consumer_client:
                asyncio.run(self.consumer_client.close())
                logger.info("Consumer client closed")

        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")

    def get_eventhub_properties(self) -> Dict[str, Any]:
        """
        Get EventHub properties and metadata.

        Returns:
            Dict[str, Any]: EventHub properties
        """
        if not self.producer_client:
            self.producer_client = self.create_producer_client()

        try:
            properties = self.producer_client.get_eventhub_properties()
            return {
                "eventhub_name": properties.name,
                "partition_count": len(properties.partition_ids),
                "partition_ids": properties.partition_ids,
                "created_at": properties.created_at
            }

        except Exception as e:
            logger.error(f"Error getting EventHub properties: {str(e)}")
            return {"error": str(e)}

class AzureStorageUtils:
    """
    Utility class for Azure Storage operations (ADLS Gen2).
    """

    @staticmethod
    def build_storage_path(storage_account: str,
                          container: str,
                          path: str,
                          use_abfss: bool = True) -> str:
        """
        Build ADLS Gen2 storage path.

        Args:
            storage_account: Storage account name
            container: Container name
            path: Relative path within container
            use_abfss: Whether to use abfss:// protocol

        Returns:
            str: Complete storage path
        """
        protocol = "abfss" if use_abfss else "abfs"
        base_path = f"{protocol}://{container}@{storage_account}.dfs.core.windows.net"

        # Ensure path starts with /
        if not path.startswith("/"):
            path = f"/{path}"

        return f"{base_path}{path}"

    @staticmethod
    def build_checkpoint_path(base_path: str, pipeline_name: str = "purchase-order-item") -> str:
        """
        Build checkpoint path for streaming.

        Args:
            base_path: Base storage path
            pipeline_name: Name of the pipeline

        Returns:
            str: Checkpoint path
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d")
        return f"{base_path}/checkpoints/{pipeline_name}/{timestamp}"

    @staticmethod
    def get_partition_path(base_path: str, date: datetime) -> str:
        """
        Get partitioned path based on date.

        Args:
            base_path: Base storage path
            date: Date for partitioning

        Returns:
            str: Partitioned path
        """
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")

        return f"{base_path}/year={year}/month={month}/day={day}"

# Mock implementations for testing

class MockEventHubProducer:
    """Mock EventHub producer for testing."""

    def create_batch(self, partition_key=None):
        return MockEventBatch()

    def send_batch(self, batch):
        logger.info(f"Mock: Sent batch with {batch.event_count} events")

    def get_eventhub_properties(self):
        from types import SimpleNamespace
        return SimpleNamespace(
            name="mock-eventhub",
            partition_ids=["0", "1", "2", "3"],
            created_at=datetime.utcnow()
        )

    def close(self):
        logger.info("Mock producer closed")

class MockEventBatch:
    """Mock event batch for testing."""

    def __init__(self):
        self.events = []
        self.event_count = 0

    def add(self, event_data):
        self.events.append(event_data)
        self.event_count += 1

class MockEventHubConsumer:
    """Mock EventHub consumer for testing."""

    async def receive(self, on_event, on_error, starting_position="-1"):
        logger.info("Mock consumer receiving events")

    async def close(self):
        logger.info("Mock consumer closed")

def create_purchase_order_event_data(purchase_order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create properly formatted event data for purchase order items.

    Args:
        purchase_order: Purchase order item data

    Returns:
        Dict[str, Any]: Formatted event data
    """
    event_data = {
        "eventType": "PurchaseOrderItemCreated",
        "eventTime": datetime.utcnow().isoformat(),
        "eventVersion": "1.0",
        "source": "purchase-order-pipeline",
        "data": purchase_order,
        "metadata": {
            "contentType": "application/json",
            "correlationId": purchase_order.get("order_id", "unknown"),
            "eventId": f"event-{int(time.time() * 1000000)}"
        }
    }

    return event_data

def validate_connection_string(connection_string: str) -> bool:
    """
    Validate EventHub connection string format.

    Args:
        connection_string: Connection string to validate

    Returns:
        bool: True if valid format
    """
    required_parts = ["Endpoint=", "SharedAccessKeyName=", "SharedAccessKey="]
    return all(part in connection_string for part in required_parts)

def extract_eventhub_name_from_connection(connection_string: str) -> Optional[str]:
    """
    Extract EventHub name from connection string if present.

    Args:
        connection_string: EventHub connection string

    Returns:
        Optional[str]: EventHub name or None
    """
    try:
        parts = connection_string.split(";")
        for part in parts:
            if part.startswith("EntityPath="):
                return part.split("=", 1)[1]
        return None

    except Exception:
        return None