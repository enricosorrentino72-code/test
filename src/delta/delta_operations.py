import structlog
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when, coalesce
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

from ..config.settings import ADLSSettings
from ..monitoring.logger import get_logger

logger = get_logger(__name__)


class DeltaLakeManager:
    """Manages Delta Lake operations for the Event Hub pipeline."""
    
    def __init__(self, spark: SparkSession, adls_settings: ADLSSettings):
        self.spark = spark
        self.adls_settings = adls_settings
        self.logger = logger.bind(component="delta_manager")
        
        self._configure_delta_lake()
    
    def _configure_delta_lake(self):
        """Configure Spark session for Delta Lake operations."""
        self.logger.info("Configuring Delta Lake")
        
        try:
            self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            self.spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            if self.adls_settings.access_key:
                self.spark.conf.set(
                    f"fs.azure.account.key.{self.adls_settings.account_name}.dfs.core.windows.net",
                    self.adls_settings.access_key
                )
            
            self.logger.info("Successfully configured Delta Lake")
            
        except Exception as e:
            self.logger.error(
                "Failed to configure Delta Lake",
                error=str(e),
                exc_info=True
            )
            raise
    
    def get_delta_table_path(self, table_name: str) -> str:
        """Get the full ADLS path for a Delta table."""
        base_path = f"abfss://{self.adls_settings.container_name}@{self.adls_settings.account_name}.dfs.core.windows.net"
        return f"{base_path}/{self.adls_settings.delta_table_path}/{table_name}"
    
    def create_events_table(self, table_name: str = "events") -> str:
        """Create the main events Delta table if it doesn't exist."""
        table_path = self.get_delta_table_path(table_name)
        
        self.logger.info(
            "Creating events Delta table",
            table_name=table_name,
            table_path=table_path
        )
        
        try:
            if DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.info("Events table already exists", table_path=table_path)
                return table_path
            
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, LongType
            
            schema = StructType([
                StructField("partition_id", StringType(), True),
                StructField("event_offset", StringType(), True),
                StructField("sequence_number", LongType(), True),
                StructField("enqueued_time", TimestampType(), True),
                StructField("processing_time", TimestampType(), True),
                StructField("event_properties", StringType(), True),
                
                StructField("event_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_time", TimestampType(), True),
                StructField("event_source", StringType(), True),
                StructField("event_subject", StringType(), True),
                StructField("event_data", StringType(), True),
                StructField("data_version", StringType(), True),
                StructField("metadata_version", StringType(), True),
                
                StructField("partition_date", StringType(), True),
                StructField("partition_hour", StringType(), True),
                
                StructField("is_valid_event_id", BooleanType(), True),
                StructField("is_valid_event_type", BooleanType(), True),
                StructField("is_valid_timestamp", BooleanType(), True),
                StructField("is_valid_source", BooleanType(), True),
                StructField("is_valid", BooleanType(), True),
                StructField("validation_time", TimestampType(), True),
                
                StructField("event_source_normalized", StringType(), True),
                StructField("event_source_domain", StringType(), True),
                StructField("effective_event_time", TimestampType(), True),
                StructField("cleansed_time", TimestampType(), True),
                
                StructField("event_category", StringType(), True),
                StructField("event_priority", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("session_id", StringType(), True),
                StructField("action", StringType(), True),
                StructField("resource", StringType(), True),
                StructField("processing_latency_seconds", LongType(), True),
                StructField("enriched_time", TimestampType(), True),
                
                StructField("delta_insert_time", TimestampType(), True),
                StructField("delta_update_time", TimestampType(), True)
            ])
            
            empty_df = self.spark.createDataFrame([], schema)
            
            (
                empty_df
                .write
                .format("delta")
                .mode("overwrite")
                .partitionBy("partition_date", "partition_hour")
                .save(table_path)
            )
            
            self.logger.info("Successfully created events Delta table", table_path=table_path)
            return table_path
            
        except Exception as e:
            self.logger.error(
                "Failed to create events Delta table",
                error=str(e),
                table_path=table_path,
                exc_info=True
            )
            raise
    
    def upsert_events(self, df: DataFrame, table_name: str = "events") -> None:
        """Upsert events into the Delta table using merge operation."""
        table_path = self.get_delta_table_path(table_name)
        
        self.logger.info(
            "Starting upsert operation",
            table_name=table_name,
            table_path=table_path
        )
        
        try:
            self.create_events_table(table_name)
            
            df_with_metadata = (
                df
                .withColumn("delta_insert_time", current_timestamp())
                .withColumn("delta_update_time", current_timestamp())
            )
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            merge_condition = (
                "target.event_id = source.event_id AND " +
                "target.partition_id = source.partition_id AND " +
                "target.event_offset = source.event_offset"
            )
            
            (
                delta_table.alias("target")
                .merge(df_with_metadata.alias("source"), merge_condition)
                .whenMatchedUpdate(set={
                    "delta_update_time": "source.delta_update_time",
                    "processing_time": "source.processing_time",
                    "validation_time": "source.validation_time",
                    "cleansed_time": "source.cleansed_time",
                    "enriched_time": "source.enriched_time"
                })
                .whenNotMatchedInsertAll()
                .execute()
            )
            
            self.logger.info("Successfully completed upsert operation", table_name=table_name)
            
        except Exception as e:
            self.logger.error(
                "Failed to upsert events",
                error=str(e),
                table_name=table_name,
                exc_info=True
            )
            raise
