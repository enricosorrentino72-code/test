# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item EventHub Listener
# MAGIC
# MAGIC Main listener class for consuming purchase order events from Azure EventHub and writing to Bronze layer.
# MAGIC
# MAGIC ## Features:
# MAGIC - Structured streaming from EventHub
# MAGIC - Bronze layer schema with technical metadata
# MAGIC - ADLS Gen2 integration
# MAGIC - Hive Metastore table management
# MAGIC - Checkpoint recovery

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, date_format,
    hour, lit, length, uuid, get_json_object
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, DateType, DoubleType
)
import logging
from typing import Dict, Any, Optional
from datetime import datetime

# COMMAND ----------

class PurchaseOrderItemListener:
    """
    Listener class for consuming purchase order events from EventHub.

    This class manages:
    - EventHub streaming connection
    - Bronze layer data transformation
    - ADLS Gen2 persistence
    - Hive table management
    - Stream monitoring
    """

    def __init__(self,
                 spark: SparkSession,
                 eventhub_config: Dict[str, str],
                 bronze_config: Dict[str, str],
                 streaming_config: Dict[str, Any],
                 log_level: str = "INFO"):
        """
        Initialize the Purchase Order Item Listener.

        Args:
            spark: Active Spark session
            eventhub_config: EventHub connection configuration
            bronze_config: Bronze layer configuration
            streaming_config: Streaming parameters
            log_level: Logging level
        """
        self.spark = spark
        self.eventhub_config = eventhub_config
        self.bronze_config = bronze_config
        self.streaming_config = streaming_config

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Initialize schemas
        self._initialize_schemas()

        # Stream query holder
        self.stream_query = None

        # Statistics
        self.stats = {
            "stream_start_time": None,
            "total_records_processed": 0,
            "last_batch_time": None,
            "errors": []
        }

    def _initialize_schemas(self):
        """Initialize the purchase order and bronze layer schemas."""
        # Purchase Order schema (from EventHub)
        self.purchase_order_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("total_amount", DoubleType(), False),
            StructField("customer_id", StringType(), False),
            StructField("vendor_id", StringType(), False),
            StructField("warehouse_location", StringType(), False),
            StructField("currency", StringType(), True),
            StructField("order_status", StringType(), False),
            StructField("payment_status", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("created_at", StringType(), False),
            StructField("order_date", StringType(), False),
            StructField("fiscal_quarter", StringType(), False),
            StructField("shipping_method", StringType(), True),
            StructField("priority_level", StringType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("tax_amount", DoubleType(), True),
            StructField("shipping_cost", DoubleType(), True),
            StructField("is_valid", StringType(), True),
            StructField("validation_errors", StringType(), True),
            StructField("data_quality_score", DoubleType(), True),
            StructField("source_system", StringType(), True),
            StructField("processing_timestamp", StringType(), True),
            StructField("cluster_id", StringType(), True),
            StructField("notebook_path", StringType(), True)
        ])

        # Bronze layer schema (with technical metadata)
        self.bronze_schema = StructType([
            # Technical metadata
            StructField("record_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False),
            StructField("ingestion_hour", IntegerType(), False),

            # EventHub metadata
            StructField("eventhub_partition", IntegerType(), True),
            StructField("eventhub_offset", LongType(), True),
            StructField("eventhub_sequence_number", LongType(), True),
            StructField("eventhub_enqueued_time", TimestampType(), True),

            # Processing metadata
            StructField("processing_cluster_id", StringType(), True),
            StructField("consumer_group", StringType(), True),
            StructField("eventhub_name", StringType(), True),

            # Payload
            StructField("raw_payload", StringType(), False),
            StructField("payload_size_bytes", IntegerType(), True),

            # Parsed business fields (for easier querying)
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("is_valid", StringType(), True),
            StructField("data_quality_score", DoubleType(), True)
        ])

    def create_eventhub_stream(self) -> DataFrame:
        """
        Create a streaming DataFrame from EventHub.

        Returns:
            DataFrame: Streaming DataFrame from EventHub
        """
        self.logger.info("Creating EventHub stream connection")

        # Build EventHub configuration
        eh_conf = {
            "eventhubs.connectionString": self.eventhub_config["connection_string"],
            "eventhubs.consumerGroup": self.eventhub_config.get("consumer_group", "$Default"),
            "eventhubs.startingPosition": json.dumps({
                "offset": "-1",
                "seqNo": -1,
                "enqueuedTime": None,
                "isInclusive": True
            }),
            "maxEventsPerTrigger": self.streaming_config.get("max_events_per_trigger", 10000)
        }

        # Create streaming DataFrame
        stream_df = (
            self.spark
            .readStream
            .format("eventhubs")
            .options(**eh_conf)
            .load()
        )

        self.logger.info("EventHub stream created successfully")
        return stream_df

    def transform_to_bronze(self, stream_df: DataFrame) -> DataFrame:
        """
        Transform EventHub stream to Bronze layer format.

        Args:
            stream_df: Raw streaming DataFrame from EventHub

        Returns:
            DataFrame: Transformed Bronze layer DataFrame
        """
        self.logger.info("Transforming stream to Bronze layer format")

        # Get cluster ID and consumer group
        cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
        consumer_group = self.eventhub_config.get("consumer_group", "$Default")
        eventhub_name = self.eventhub_config.get("eventhub_name", "unknown")

        # Transform to Bronze format
        bronze_df = (
            stream_df
            # Add technical metadata
            .withColumn("record_id", uuid())
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", date_format(col("ingestion_timestamp"), "yyyy-MM-dd").cast(DateType()))
            .withColumn("ingestion_hour", hour(col("ingestion_timestamp")))

            # EventHub metadata
            .withColumn("eventhub_partition", col("partition").cast(IntegerType()))
            .withColumn("eventhub_offset", col("offset").cast(LongType()))
            .withColumn("eventhub_sequence_number", col("sequenceNumber").cast(LongType()))
            .withColumn("eventhub_enqueued_time", col("enqueuedTime"))

            # Processing metadata
            .withColumn("processing_cluster_id", lit(cluster_id))
            .withColumn("consumer_group", lit(consumer_group))
            .withColumn("eventhub_name", lit(eventhub_name))

            # Payload
            .withColumn("raw_payload", col("body").cast(StringType()))
            .withColumn("payload_size_bytes", length(col("raw_payload")))

            # Extract key business fields for easier querying
            .withColumn("order_id", get_json_object(col("raw_payload"), "$.order_id"))
            .withColumn("customer_id", get_json_object(col("raw_payload"), "$.customer_id"))
            .withColumn("order_status", get_json_object(col("raw_payload"), "$.order_status"))
            .withColumn("payment_status", get_json_object(col("raw_payload"), "$.payment_status"))
            .withColumn("total_amount",
                       get_json_object(col("raw_payload"), "$.total_amount").cast(DoubleType()))
            .withColumn("is_valid", get_json_object(col("raw_payload"), "$.is_valid"))
            .withColumn("data_quality_score",
                       get_json_object(col("raw_payload"), "$.data_quality_score").cast(DoubleType()))

            # Select final columns in Bronze schema order
            .select(
                "record_id", "ingestion_timestamp", "ingestion_date", "ingestion_hour",
                "eventhub_partition", "eventhub_offset", "eventhub_sequence_number", "eventhub_enqueued_time",
                "processing_cluster_id", "consumer_group", "eventhub_name",
                "raw_payload", "payload_size_bytes",
                "order_id", "customer_id", "order_status", "payment_status",
                "total_amount", "is_valid", "data_quality_score"
            )
        )

        return bronze_df

    def start_streaming(self,
                       output_mode: str = "append",
                       trigger_interval: str = "5 seconds") -> Any:
        """
        Start the streaming process from EventHub to Bronze layer.

        Args:
            output_mode: Spark output mode (append, complete, update)
            trigger_interval: Trigger interval for micro-batches

        Returns:
            StreamingQuery object
        """
        self.logger.info(f"Starting streaming with trigger interval: {trigger_interval}")
        self.stats["stream_start_time"] = datetime.utcnow()

        try:
            # Create EventHub stream
            stream_df = self.create_eventhub_stream()

            # Transform to Bronze format
            bronze_df = self.transform_to_bronze(stream_df)

            # Build output path with partitioning
            output_path = self.bronze_config["bronze_path"]
            checkpoint_path = self.bronze_config["checkpoint_path"]

            # Start the stream with foreachBatch for monitoring
            self.stream_query = (
                bronze_df
                .writeStream
                .outputMode(output_mode)
                .trigger(processingTime=trigger_interval)
                .option("checkpointLocation", checkpoint_path)
                .foreachBatch(self._process_batch)
                .start()
            )

            self.logger.info(f"Stream started successfully. Query ID: {self.stream_query.id}")
            self.logger.info(f"Writing to: {output_path}")
            self.logger.info(f"Checkpoint location: {checkpoint_path}")

            return self.stream_query

        except Exception as e:
            self.logger.error(f"Failed to start streaming: {e}")
            self.stats["errors"].append(str(e))
            raise

    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process each micro-batch with monitoring and error handling.

        Args:
            batch_df: DataFrame for current batch
            batch_id: Unique batch identifier
        """
        try:
            batch_count = batch_df.count()
            self.logger.info(f"Processing batch {batch_id} with {batch_count} records")

            if batch_count > 0:
                # Write to Bronze layer with partitioning
                output_path = self.bronze_config["bronze_path"]

                (
                    batch_df
                    .write
                    .mode("append")
                    .partitionBy("ingestion_date", "ingestion_hour")
                    .parquet(output_path)
                )

                # Update statistics
                self.stats["total_records_processed"] += batch_count
                self.stats["last_batch_time"] = datetime.utcnow()

                # Log sample records for debugging
                if self.logger.level <= logging.DEBUG:
                    sample_records = batch_df.select("record_id", "order_id", "order_status").limit(5).collect()
                    for record in sample_records:
                        self.logger.debug(f"Sample: {record}")

                # Check for data quality issues
                quality_issues = batch_df.filter(col("is_valid") == "False").count()
                if quality_issues > 0:
                    self.logger.warning(f"Batch {batch_id} contains {quality_issues} records with quality issues")

                self.logger.info(f"Batch {batch_id} written successfully. Total processed: {self.stats['total_records_processed']}")

        except Exception as e:
            self.logger.error(f"Error processing batch {batch_id}: {e}")
            self.stats["errors"].append(f"Batch {batch_id}: {str(e)}")
            raise

    def stop_streaming(self):
        """Stop the streaming query gracefully."""
        if self.stream_query:
            try:
                self.logger.info("Stopping streaming query...")
                self.stream_query.stop()
                self.logger.info("Streaming query stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping stream: {e}")

    def get_stream_status(self) -> Dict[str, Any]:
        """
        Get current streaming status and statistics.

        Returns:
            Dictionary with stream status information
        """
        status = {
            "is_active": False,
            "statistics": self.stats
        }

        if self.stream_query:
            status.update({
                "is_active": self.stream_query.isActive,
                "query_id": self.stream_query.id,
                "run_id": self.stream_query.runId,
                "last_progress": self.stream_query.lastProgress
            })

            if self.stream_query.isActive:
                recent_progress = self.stream_query.recentProgress
                if recent_progress:
                    status["recent_batches"] = [
                        {
                            "id": p.get("id"),
                            "timestamp": p.get("timestamp"),
                            "input_rows": p.get("inputRowsPerSecond"),
                            "processed_rows": p.get("processedRowsPerSecond")
                        }
                        for p in recent_progress[-5:]  # Last 5 batches
                    ]

        return status

    def await_termination(self, timeout: Optional[int] = None):
        """
        Wait for the stream to terminate.

        Args:
            timeout: Optional timeout in seconds
        """
        if self.stream_query:
            try:
                if timeout:
                    self.stream_query.awaitTermination(timeout)
                else:
                    self.stream_query.awaitTermination()
            except KeyboardInterrupt:
                self.logger.info("Stream interrupted by user")
                self.stop_streaming()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_listener_from_widgets(spark, dbutils) -> PurchaseOrderItemListener:
    """
    Create a listener instance from Databricks widget values.

    Args:
        spark: Spark session
        dbutils: Databricks utilities

    Returns:
        Configured PurchaseOrderItemListener instance
    """
    # Get widget values
    SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
    EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
    CONSUMER_GROUP = dbutils.widgets.get("consumer_group")
    BRONZE_PATH = dbutils.widgets.get("bronze_path")
    CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
    MAX_EVENTS = int(dbutils.widgets.get("max_events_per_trigger"))
    LOG_LEVEL = dbutils.widgets.get("log_level")

    # Get connection string from secret scope
    connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{EVENTHUB_NAME}-connection-string")

    # Build configuration
    eventhub_config = {
        "connection_string": connection_string,
        "eventhub_name": EVENTHUB_NAME,
        "consumer_group": CONSUMER_GROUP
    }

    bronze_config = {
        "bronze_path": BRONZE_PATH,
        "checkpoint_path": CHECKPOINT_PATH
    }

    streaming_config = {
        "max_events_per_trigger": MAX_EVENTS
    }

    # Create and return listener
    return PurchaseOrderItemListener(
        spark=spark,
        eventhub_config=eventhub_config,
        bronze_config=bronze_config,
        streaming_config=streaming_config,
        log_level=LOG_LEVEL
    )

# COMMAND ----------

# Export for use in other notebooks
__all__ = ['PurchaseOrderItemListener', 'create_listener_from_widgets']