import structlog
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, trim, upper, lower,
    from_json, to_json, current_timestamp, lit, coalesce, 
    regexp_extract, split, size, array_contains
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from ..monitoring.logger import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """Data processing and transformation logic for Event Hub events."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logger.bind(component="data_processor")
    
    def validate_events(self, df: DataFrame) -> DataFrame:
        """Validate incoming events and mark invalid ones."""
        self.logger.info("Starting event validation")
        
        try:
            validated_df = (
                df
                .withColumn(
                    "is_valid_event_id",
                    when(
                        (col("event_id").isNotNull()) & 
                        (col("event_id") != "") & 
                        (col("event_id").rlike("^[a-zA-Z0-9-]+$")),
                        lit(True)
                    ).otherwise(lit(False))
                )
                .withColumn(
                    "is_valid_event_type",
                    when(
                        (col("event_type").isNotNull()) & 
                        (col("event_type") != ""),
                        lit(True)
                    ).otherwise(lit(False))
                )
                .withColumn(
                    "is_valid_timestamp",
                    when(
                        col("event_time").isNotNull() | col("enqueued_time").isNotNull(),
                        lit(True)
                    ).otherwise(lit(False))
                )
                .withColumn(
                    "is_valid_source",
                    when(
                        (col("event_source").isNotNull()) & 
                        (col("event_source") != ""),
                        lit(True)
                    ).otherwise(lit(False))
                )
                .withColumn(
                    "is_valid",
                    col("is_valid_event_id") & 
                    col("is_valid_event_type") & 
                    col("is_valid_timestamp") & 
                    col("is_valid_source")
                )
                .withColumn("validation_time", current_timestamp())
            )
            
            self.logger.info("Successfully completed event validation")
            return validated_df
            
        except Exception as e:
            self.logger.error(
                "Failed to validate events",
                error=str(e),
                exc_info=True
            )
            raise
    
    def cleanse_data(self, df: DataFrame) -> DataFrame:
        """Cleanse and standardize event data."""
        self.logger.info("Starting data cleansing")
        
        try:
            cleansed_df = (
                df
                .withColumn("event_id", trim(col("event_id")))
                .withColumn("event_type", trim(upper(col("event_type"))))
                .withColumn("event_source", trim(col("event_source")))
                .withColumn("event_subject", trim(col("event_subject")))
                .withColumn("data_version", trim(col("data_version")))
                .withColumn("metadata_version", trim(col("metadata_version")))
                
                .withColumn(
                    "event_source_normalized",
                    regexp_replace(lower(col("event_source")), "[^a-z0-9.]", "")
                )
                
                .withColumn(
                    "event_source_domain",
                    when(
                        col("event_source").rlike("^https?://"),
                        regexp_extract(col("event_source"), "^https?://([^/]+)", 1)
                    ).otherwise(col("event_source_normalized"))
                )
                
                .withColumn(
                    "effective_event_time",
                    coalesce(col("event_time"), col("enqueued_time"))
                )
                
                .withColumn("cleansed_time", current_timestamp())
            )
            
            self.logger.info("Successfully completed data cleansing")
            return cleansed_df
            
        except Exception as e:
            self.logger.error(
                "Failed to cleanse data",
                error=str(e),
                exc_info=True
            )
            raise
    
    def enrich_events(self, df: DataFrame) -> DataFrame:
        """Enrich events with additional metadata and derived fields."""
        self.logger.info("Starting event enrichment")
        
        try:
            enriched_df = (
                df
                .withColumn(
                    "event_category",
                    when(col("event_type").rlike("(?i).*user.*"), "USER")
                    .when(col("event_type").rlike("(?i).*system.*"), "SYSTEM")
                    .when(col("event_type").rlike("(?i).*error.*"), "ERROR")
                    .when(col("event_type").rlike("(?i).*security.*"), "SECURITY")
                    .when(col("event_type").rlike("(?i).*audit.*"), "AUDIT")
                    .otherwise("OTHER")
                )
                
                .withColumn(
                    "event_priority",
                    when(
                        col("event_type").rlike("(?i).*(critical|urgent|high).*") |
                        col("event_subject").rlike("(?i).*(critical|urgent|high).*"),
                        "HIGH"
                    )
                    .when(
                        col("event_type").rlike("(?i).*(medium|normal).*") |
                        col("event_subject").rlike("(?i).*(medium|normal).*"),
                        "MEDIUM"
                    )
                    .otherwise("LOW")
                )
                
                .withColumn(
                    "event_data_parsed",
                    when(
                        col("event_data").rlike("^\\s*[{\\[].*[}\\]]\\s*$"),
                        from_json(col("event_data"), StructType([
                            StructField("userId", StringType(), True),
                            StructField("sessionId", StringType(), True),
                            StructField("action", StringType(), True),
                            StructField("resource", StringType(), True),
                            StructField("metadata", StringType(), True)
                        ]))
                    ).otherwise(lit(None))
                )
                
                .withColumn("user_id", col("event_data_parsed.userId"))
                .withColumn("session_id", col("event_data_parsed.sessionId"))
                .withColumn("action", col("event_data_parsed.action"))
                .withColumn("resource", col("event_data_parsed.resource"))
                
                .withColumn(
                    "processing_latency_seconds",
                    (col("processing_time").cast("long") - col("enqueued_time").cast("long"))
                )
                
                .withColumn("enriched_time", current_timestamp())
            )
            
            self.logger.info("Successfully completed event enrichment")
            return enriched_df
            
        except Exception as e:
            self.logger.error(
                "Failed to enrich events",
                error=str(e),
                exc_info=True
            )
            raise
    
    def filter_valid_events(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Separate valid and invalid events."""
        self.logger.info("Filtering valid and invalid events")
        
        try:
            valid_events = df.filter(col("is_valid") == True)
            invalid_events = df.filter(col("is_valid") == False)
            
            valid_count = valid_events.count() if hasattr(valid_events, 'count') else "unknown"
            invalid_count = invalid_events.count() if hasattr(invalid_events, 'count') else "unknown"
            
            self.logger.info(
                "Successfully filtered events",
                valid_events=valid_count,
                invalid_events=invalid_count
            )
            
            return valid_events, invalid_events
            
        except Exception as e:
            self.logger.error(
                "Failed to filter events",
                error=str(e),
                exc_info=True
            )
            raise
    
    def process_events(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Complete event processing pipeline."""
        self.logger.info("Starting complete event processing pipeline")
        
        try:
            validated_df = self.validate_events(df)
            
            cleansed_df = self.cleanse_data(validated_df)
            
            enriched_df = self.enrich_events(cleansed_df)
            
            valid_events, invalid_events = self.filter_valid_events(enriched_df)
            
            self.logger.info("Successfully completed event processing pipeline")
            return valid_events, invalid_events
            
        except Exception as e:
            self.logger.error(
                "Failed to process events",
                error=str(e),
                exc_info=True
            )
            raise
    
    def create_dead_letter_record(self, invalid_df: DataFrame) -> DataFrame:
        """Create dead letter records for invalid events."""
        self.logger.info("Creating dead letter records")
        
        try:
            dead_letter_df = (
                invalid_df
                .withColumn("dead_letter_reason", 
                    when(~col("is_valid_event_id"), "INVALID_EVENT_ID")
                    .when(~col("is_valid_event_type"), "INVALID_EVENT_TYPE")
                    .when(~col("is_valid_timestamp"), "INVALID_TIMESTAMP")
                    .when(~col("is_valid_source"), "INVALID_SOURCE")
                    .otherwise("UNKNOWN_VALIDATION_ERROR")
                )
                .withColumn("dead_letter_time", current_timestamp())
                .withColumn("retry_count", lit(0))
                .withColumn("max_retries", lit(3))
            )
            
            self.logger.info("Successfully created dead letter records")
            return dead_letter_df
            
        except Exception as e:
            self.logger.error(
                "Failed to create dead letter records",
                error=str(e),
                exc_info=True
            )
            raise
