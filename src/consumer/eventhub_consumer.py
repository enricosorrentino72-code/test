import structlog
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

from ..config.settings import EventHubSettings, SparkSettings
from ..monitoring.logger import get_logger

logger = get_logger(__name__)


class EventHubConsumer:
    """Event Hub consumer using Spark Structured Streaming."""
    
    def __init__(
        self,
        spark: SparkSession,
        eventhub_settings: EventHubSettings,
        spark_settings: SparkSettings
    ):
        self.spark = spark
        self.eventhub_settings = eventhub_settings
        self.spark_settings = spark_settings
        self.logger = logger.bind(component="eventhub_consumer")
        
    def get_event_schema(self) -> StructType:
        """Define the schema for incoming events."""
        return StructType([
            StructField("eventId", StringType(), True),
            StructField("eventType", StringType(), True),
            StructField("eventTime", TimestampType(), True),
            StructField("source", StringType(), True),
            StructField("subject", StringType(), True),
            StructField("data", StringType(), True),  # JSON string
            StructField("dataVersion", StringType(), True),
            StructField("metadataVersion", StringType(), True)
        ])
    
    def create_eventhub_stream(self) -> DataFrame:
        """Create a streaming DataFrame from Event Hub."""
        self.logger.info(
            "Creating Event Hub stream",
            event_hub_name=self.eventhub_settings.event_hub_name,
            consumer_group=self.eventhub_settings.consumer_group
        )
        
        eventhub_conf = {
            "eventhubs.connectionString": self.eventhub_settings.connection_string,
            "eventhubs.consumerGroup": self.eventhub_settings.consumer_group,
            "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
            "eventhubs.maxEventsPerTrigger": str(self.spark_settings.max_offsets_per_trigger)
        }
        
        try:
            df = (
                self.spark
                .readStream
                .format("eventhubs")
                .options(**eventhub_conf)
                .load()
            )
            
            self.logger.info("Successfully created Event Hub stream")
            return df
            
        except Exception as e:
            self.logger.error(
                "Failed to create Event Hub stream",
                error=str(e),
                exc_info=True
            )
            raise
    
    def parse_event_data(self, df: DataFrame) -> DataFrame:
        """Parse and structure the Event Hub data."""
        self.logger.info("Parsing Event Hub data")
        
        try:
            
            parsed_df = (
                df
                .select(
                    col("body").cast("string").alias("event_body"),
                    col("partition").alias("partition_id"),
                    col("offset").alias("event_offset"),
                    col("sequenceNumber").alias("sequence_number"),
                    col("enqueuedTime").alias("enqueued_time"),
                    col("properties").alias("event_properties"),
                    current_timestamp().alias("processing_time")
                )
                .select(
                    "*",
                    from_json(col("event_body"), self.get_event_schema()).alias("parsed_event")
                )
                .select(
                    col("partition_id"),
                    col("event_offset"),
                    col("sequence_number"),
                    col("enqueued_time"),
                    col("processing_time"),
                    col("event_properties"),
                    col("parsed_event.eventId").alias("event_id"),
                    col("parsed_event.eventType").alias("event_type"),
                    col("parsed_event.eventTime").alias("event_time"),
                    col("parsed_event.source").alias("event_source"),
                    col("parsed_event.subject").alias("event_subject"),
                    col("parsed_event.data").alias("event_data"),
                    col("parsed_event.dataVersion").alias("data_version"),
                    col("parsed_event.metadataVersion").alias("metadata_version")
                )
                .withColumn(
                    "partition_date",
                    expr("date_format(coalesce(event_time, enqueued_time), 'yyyy-MM-dd')")
                )
                .withColumn(
                    "partition_hour",
                    expr("date_format(coalesce(event_time, enqueued_time), 'HH')")
                )
            )
            
            self.logger.info("Successfully parsed Event Hub data")
            return parsed_df
            
        except Exception as e:
            self.logger.error(
                "Failed to parse Event Hub data",
                error=str(e),
                exc_info=True
            )
            raise
    
    def create_streaming_query(
        self,
        df: DataFrame,
        output_path: str,
        checkpoint_location: Optional[str] = None
    ) -> Any:
        """Create a streaming query to write data to Delta Lake."""
        checkpoint_path = checkpoint_location or self.eventhub_settings.checkpoint_location
        
        self.logger.info(
            "Creating streaming query",
            output_path=output_path,
            checkpoint_location=checkpoint_path,
            trigger_interval=self.spark_settings.trigger_interval
        )
        
        try:
            query = (
                df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .option("path", output_path)
                .partitionBy("partition_date", "partition_hour")
                .trigger(processingTime=self.spark_settings.trigger_interval)
            )
            
            self.logger.info("Successfully created streaming query")
            return query
            
        except Exception as e:
            self.logger.error(
                "Failed to create streaming query",
                error=str(e),
                exc_info=True
            )
            raise
    
    def start_streaming(self, output_path: str) -> Any:
        """Start the complete streaming pipeline."""
        self.logger.info("Starting Event Hub streaming pipeline")
        
        try:
            raw_stream = self.create_eventhub_stream()
            
            parsed_stream = self.parse_event_data(raw_stream)
            
            query = self.create_streaming_query(parsed_stream, output_path)
            streaming_query = query.start()
            
            self.logger.info(
                "Successfully started streaming pipeline",
                query_id=streaming_query.id,
                query_name=streaming_query.name
            )
            
            return streaming_query
            
        except Exception as e:
            self.logger.error(
                "Failed to start streaming pipeline",
                error=str(e),
                exc_info=True
            )
            raise
