# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item Producer
# MAGIC
# MAGIC Main producer class for sending purchase order data to Azure EventHub.
# MAGIC
# MAGIC ## Features:
# MAGIC - Batch sending with configurable size
# MAGIC - Retry logic with exponential backoff
# MAGIC - Performance monitoring
# MAGIC - Error handling and recovery
# MAGIC - Databricks integration

# COMMAND ----------

import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch
from azure.eventhub.exceptions import EventHubError
import concurrent.futures

# COMMAND ----------

# Import required classes
from purchase_order_item_model import PurchaseOrderItem
from purchase_order_item_factory import PurchaseOrderItemFactory

# COMMAND ----------

class PurchaseOrderItemProducer:
    """
    Producer class for sending purchase order items to Azure EventHub.

    This class manages the complete lifecycle of producing purchase order events:
    - Connection management
    - Batch optimization
    - Error handling and retries
    - Performance monitoring
    """

    def __init__(self,
                 connection_string: str,
                 eventhub_name: str,
                 batch_size: int = 50,
                 send_interval: float = 2.0,
                 max_retries: int = 3,
                 log_level: str = "INFO"):
        """
        Initialize the Purchase Order Item Producer.

        Args:
            connection_string: EventHub connection string
            eventhub_name: Name of the EventHub
            batch_size: Number of events per batch
            send_interval: Seconds between batch sends
            max_retries: Maximum retry attempts
            log_level: Logging level
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.batch_size = batch_size
        self.send_interval = send_interval
        self.max_retries = max_retries

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Initialize producer client
        self.producer_client = None
        self._initialize_client()

        # Initialize factory for data generation
        self.factory = PurchaseOrderItemFactory(quality_issue_rate=0.05)

        # Statistics tracking
        self.stats = {
            "total_events_sent": 0,
            "total_batches_sent": 0,
            "failed_events": 0,
            "retry_count": 0,
            "start_time": None,
            "total_bytes_sent": 0,
            "quality_issues_generated": 0
        }

    def _initialize_client(self):
        """Initialize or reinitialize the EventHub producer client."""
        try:
            if self.producer_client:
                self.producer_client.close()

            self.producer_client = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            self.logger.info(f"Successfully connected to EventHub: {self.eventhub_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize EventHub client: {e}")
            raise

    def _create_event_batch(self, orders: List[PurchaseOrderItem]) -> EventDataBatch:
        """
        Create an EventDataBatch from purchase orders.

        Args:
            orders: List of purchase orders

        Returns:
            EventDataBatch ready to send
        """
        batch = self.producer_client.create_batch()

        for order in orders:
            try:
                # Add processing timestamp
                order.processing_timestamp = datetime.utcnow()

                # Convert to JSON
                event_body = order.to_json()

                # Create event with metadata
                event = EventData(event_body)
                event.properties = {
                    "order_id": order.order_id,
                    "order_status": order.order_status,
                    "payment_status": order.payment_status,
                    "customer_id": order.customer_id,
                    "is_valid": str(order.is_valid),
                    "data_quality_score": str(order.data_quality_score)
                }

                # Try to add to batch
                batch.add(event)

                # Track statistics
                self.stats["total_bytes_sent"] += len(event_body)
                if not order.is_valid:
                    self.stats["quality_issues_generated"] += 1

            except ValueError:
                # Batch is full, return what we have
                self.logger.warning(f"Batch full at {len(batch)} events")
                break
            except Exception as e:
                self.logger.error(f"Error adding event to batch: {e}")
                self.stats["failed_events"] += 1

        return batch

    def _send_batch_with_retry(self, batch: EventDataBatch) -> bool:
        """
        Send a batch with retry logic.

        Args:
            batch: EventDataBatch to send

        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.max_retries):
            try:
                self.producer_client.send_batch(batch)
                self.logger.debug(f"Successfully sent batch of {len(batch)} events")
                return True

            except EventHubError as e:
                self.stats["retry_count"] += 1
                wait_time = 2 ** attempt  # Exponential backoff
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

            except Exception as e:
                self.logger.error(f"Unexpected error sending batch: {e}")
                break

        self.logger.error(f"Failed to send batch after {self.max_retries} attempts")
        return False

    def produce_continuous(self, duration_minutes: int = 60, scenario: str = "normal") -> Dict[str, Any]:
        """
        Continuously produce purchase order events for a specified duration.

        Args:
            duration_minutes: How long to produce events
            scenario: Scenario type for data generation

        Returns:
            Dictionary with production statistics
        """
        self.stats["start_time"] = datetime.utcnow()
        end_time = self.stats["start_time"] + timedelta(minutes=duration_minutes)

        self.logger.info(f"Starting continuous production for {duration_minutes} minutes")
        self.logger.info(f"Batch size: {self.batch_size}, Send interval: {self.send_interval}s")

        try:
            while datetime.utcnow() < end_time:
                # Generate batch of orders
                if scenario == "seasonal":
                    season = random.choice(["holiday", "back_to_school", "black_friday", "normal"])
                    orders = self.factory.generate_seasonal_batch(self.batch_size, season)
                else:
                    orders = self.factory.generate_batch(self.batch_size)

                # Create and send batch
                batch = self._create_event_batch(orders)

                if len(batch) > 0:
                    if self._send_batch_with_retry(batch):
                        self.stats["total_events_sent"] += len(batch)
                        self.stats["total_batches_sent"] += 1
                    else:
                        self.stats["failed_events"] += len(batch)

                # Log progress every 10 batches
                if self.stats["total_batches_sent"] % 10 == 0:
                    self._log_progress()

                # Wait before next batch
                time.sleep(self.send_interval)

        except KeyboardInterrupt:
            self.logger.info("Production interrupted by user")
        except Exception as e:
            self.logger.error(f"Error during continuous production: {e}")
        finally:
            self._finalize_production()

        return self.get_statistics()

    def produce_batch_async(self, total_events: int = 1000) -> Dict[str, Any]:
        """
        Produce a specific number of events using async batch sending.

        Args:
            total_events: Total number of events to send

        Returns:
            Dictionary with production statistics
        """
        self.stats["start_time"] = datetime.utcnow()
        events_remaining = total_events

        self.logger.info(f"Starting batch production of {total_events} events")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []

            while events_remaining > 0:
                batch_size = min(self.batch_size, events_remaining)
                orders = self.factory.generate_batch(batch_size)
                batch = self._create_event_batch(orders)

                # Submit batch for async sending
                future = executor.submit(self._send_batch_with_retry, batch)
                futures.append((future, len(batch)))

                events_remaining -= batch_size

            # Wait for all batches to complete
            for future, batch_size in futures:
                if future.result():
                    self.stats["total_events_sent"] += batch_size
                    self.stats["total_batches_sent"] += 1
                else:
                    self.stats["failed_events"] += batch_size

        self._finalize_production()
        return self.get_statistics()

    def produce_scenario_test(self) -> Dict[str, Any]:
        """
        Produce test batches with different scenarios for validation.

        Returns:
            Dictionary with test results
        """
        self.logger.info("Starting scenario test production")
        test_results = {}

        scenarios = [
            ("normal", 100),
            ("bulk_order", 50),
            ("high_value", 50),
            ("quality_issues", 100)
        ]

        for scenario, count in scenarios:
            self.logger.info(f"Testing scenario: {scenario}")

            if scenario == "quality_issues":
                # Force quality issues
                orders = [self.factory.generate_single_item(inject_quality_issue=True) for _ in range(count)]
            else:
                orders = [self.factory.generate_single_item(specific_scenario=scenario) for _ in range(count)]

            batch = self._create_event_batch(orders)
            success = self._send_batch_with_retry(batch)

            test_results[scenario] = {
                "events_sent": len(batch),
                "success": success,
                "statistics": self.factory.get_statistics(orders)
            }

            time.sleep(1)  # Brief pause between scenarios

        return test_results

    def _log_progress(self):
        """Log current production progress."""
        elapsed = (datetime.utcnow() - self.stats["start_time"]).total_seconds()
        rate = self.stats["total_events_sent"] / elapsed if elapsed > 0 else 0

        self.logger.info(f"Progress: {self.stats['total_events_sent']} events sent "
                        f"({self.stats['total_batches_sent']} batches) "
                        f"at {rate:.2f} events/sec")
        self.logger.info(f"Quality issues: {self.stats['quality_issues_generated']}, "
                        f"Failed: {self.stats['failed_events']}, "
                        f"Retries: {self.stats['retry_count']}")

    def _finalize_production(self):
        """Finalize production and clean up resources."""
        try:
            if self.producer_client:
                self.producer_client.close()
                self.logger.info("Producer client closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing producer client: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get production statistics.

        Returns:
            Dictionary with comprehensive statistics
        """
        if self.stats["start_time"]:
            elapsed = (datetime.utcnow() - self.stats["start_time"]).total_seconds()
            rate = self.stats["total_events_sent"] / elapsed if elapsed > 0 else 0
            mb_sent = self.stats["total_bytes_sent"] / (1024 * 1024)
        else:
            elapsed = 0
            rate = 0
            mb_sent = 0

        return {
            "total_events_sent": self.stats["total_events_sent"],
            "total_batches_sent": self.stats["total_batches_sent"],
            "failed_events": self.stats["failed_events"],
            "retry_count": self.stats["retry_count"],
            "quality_issues_generated": self.stats["quality_issues_generated"],
            "elapsed_seconds": round(elapsed, 2),
            "events_per_second": round(rate, 2),
            "total_mb_sent": round(mb_sent, 2),
            "success_rate": (self.stats["total_events_sent"] /
                           (self.stats["total_events_sent"] + self.stats["failed_events"]))
                          if (self.stats["total_events_sent"] + self.stats["failed_events"]) > 0 else 0
        }

    def test_connection(self) -> bool:
        """
        Test the EventHub connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to create a batch
            test_batch = self.producer_client.create_batch()
            self.logger.info("EventHub connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"EventHub connection test failed: {e}")
            return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def create_producer_from_widgets(dbutils, spark) -> PurchaseOrderItemProducer:
    """
    Create a producer instance from Databricks widget values.

    Args:
        dbutils: Databricks utilities
        spark: Spark session

    Returns:
        Configured PurchaseOrderItemProducer instance
    """
    # Get widget values
    SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
    EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
    BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
    SEND_INTERVAL = float(dbutils.widgets.get("send_interval"))
    LOG_LEVEL = dbutils.widgets.get("log_level")

    # Get connection string from secret scope
    connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{EVENTHUB_NAME}-connection-string")

    # Create and return producer
    return PurchaseOrderItemProducer(
        connection_string=connection_string,
        eventhub_name=EVENTHUB_NAME,
        batch_size=BATCH_SIZE,
        send_interval=SEND_INTERVAL,
        log_level=LOG_LEVEL
    )

# COMMAND ----------

# Export for use in other notebooks
__all__ = ['PurchaseOrderItemProducer', 'create_producer_from_widgets']