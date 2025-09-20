# Databricks Utilities for Purchase Order Item Pipeline

import logging
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, current_timestamp, lit, from_json, to_timestamp, date_format, hour

logger = logging.getLogger(__name__)

class DatabricksUtils:
    """
    Utility class for Databricks operations specific to Purchase Order Item pipeline.

    Provides common functionality for Spark operations, schema management,
    and Databricks-specific configurations.
    """

    @staticmethod
    def get_purchase_order_schema() -> StructType:
        """
        Get the schema definition for Purchase Order Item data.

        Returns:
            StructType: Spark schema for purchase order items
        """
        return StructType([
            # Core identification fields
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False),

            # Product information
            StructField("product_name", StringType(), False),
            StructField("product_category", StringType(), True),
            StructField("product_description", StringType(), True),

            # Quantity and pricing
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("total_amount", DecimalType(10, 2), False),

            # Customer and vendor information
            StructField("customer_id", StringType(), False),
            StructField("vendor_id", StringType(), False),

            # Location and logistics
            StructField("warehouse_location", StringType(), False),
            StructField("shipping_address", StringType(), True),

            # Business fields
            StructField("currency", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("payment_status", StringType(), False),

            # Timestamps
            StructField("timestamp", TimestampType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), True),

            # Discounts and taxes
            StructField("discount_percentage", DecimalType(5, 2), False),
            StructField("tax_rate", DecimalType(5, 2), False),

            # Additional metadata
            StructField("priority_level", StringType(), False),
            StructField("notes", StringType(), True),

            # DQX quality fields (added by pipeline)
            StructField("dqx_check_results", StringType(), True),
            StructField("dqx_valid", BooleanType(), True),
            StructField("dqx_timestamp", TimestampType(), True)
        ])

    @staticmethod
    def create_database_if_not_exists(spark: SparkSession, database_name: str) -> None:
        """
        Create database if it doesn't exist.

        Args:
            spark: Spark session
            database_name: Name of the database to create
        """
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            logger.info(f"Database {database_name} created or already exists")
        except Exception as e:
            logger.error(f"Error creating database {database_name}: {str(e)}")
            raise

    @staticmethod
    def create_purchase_order_table(spark: SparkSession,
                                  database_name: str,
                                  table_name: str,
                                  storage_path: str,
                                  is_bronze: bool = True) -> None:
        """
        Create purchase order table with proper schema and partitioning.

        Args:
            spark: Spark session
            database_name: Database name
            table_name: Table name
            storage_path: Storage path for the table
            is_bronze: Whether this is a bronze layer table (affects schema)
        """
        try:
            # Use the database
            spark.sql(f"USE {database_name}")

            # Build CREATE TABLE statement
            schema = DatabricksUtils.get_purchase_order_schema()

            columns_ddl = []
            for field in schema.fields:
                nullable = "NULL" if field.nullable else "NOT NULL"

                if field.dataType == StringType():
                    col_type = "STRING"
                elif field.dataType == IntegerType():
                    col_type = "INT"
                elif isinstance(field.dataType, DecimalType):
                    col_type = f"DECIMAL({field.dataType.precision},{field.dataType.scale})"
                elif field.dataType == TimestampType():
                    col_type = "TIMESTAMP"
                elif field.dataType == BooleanType():
                    col_type = "BOOLEAN"
                else:
                    col_type = "STRING"

                columns_ddl.append(f"  {field.name} {col_type} {nullable}")

            columns_str = ",\n".join(columns_ddl)

            # Create table with partitioning
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {columns_str}
            )
            USING DELTA
            LOCATION '{storage_path}'
            PARTITIONED BY (DATE(created_at))
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
            """

            spark.sql(create_table_sql)
            logger.info(f"Table {database_name}.{table_name} created successfully")

        except Exception as e:
            logger.error(f"Error creating table {database_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def optimize_table(spark: SparkSession, database_name: str, table_name: str) -> None:
        """
        Optimize Delta table for better query performance.

        Args:
            spark: Spark session
            database_name: Database name
            table_name: Table name
        """
        try:
            # Optimize table
            spark.sql(f"OPTIMIZE {database_name}.{table_name}")

            # Z-order by commonly used columns
            spark.sql(f"OPTIMIZE {database_name}.{table_name} ZORDER BY (order_id, customer_id, created_at)")

            logger.info(f"Table {database_name}.{table_name} optimized successfully")

        except Exception as e:
            logger.error(f"Error optimizing table {database_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def add_processing_metadata(df: DataFrame,
                              pipeline_stage: str = "bronze",
                              job_id: Optional[str] = None) -> DataFrame:
        """
        Add processing metadata to DataFrame.

        Args:
            df: Input DataFrame
            pipeline_stage: Stage of the pipeline (bronze, silver, etc.)
            job_id: Optional job identifier

        Returns:
            DataFrame: DataFrame with added metadata
        """
        try:
            result_df = df.withColumn("processing_timestamp", current_timestamp()) \
                         .withColumn("pipeline_stage", lit(pipeline_stage))

            if job_id:
                result_df = result_df.withColumn("job_id", lit(job_id))

            return result_df

        except Exception as e:
            logger.error(f"Error adding processing metadata: {str(e)}")
            raise

    @staticmethod
    def get_table_info(spark: SparkSession, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.

        Args:
            spark: Spark session
            database_name: Database name
            table_name: Table name

        Returns:
            Dict[str, Any]: Table information
        """
        try:
            # Get table details
            describe_df = spark.sql(f"DESCRIBE DETAIL {database_name}.{table_name}")
            table_info = describe_df.collect()[0].asDict()

            # Get record count
            count_df = spark.sql(f"SELECT COUNT(*) as record_count FROM {database_name}.{table_name}")
            record_count = count_df.collect()[0]["record_count"]

            table_info["record_count"] = record_count

            return table_info

        except Exception as e:
            logger.error(f"Error getting table info for {database_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def vacuum_table(spark: SparkSession,
                    database_name: str,
                    table_name: str,
                    retention_hours: int = 168) -> None:
        """
        Vacuum old files from Delta table.

        Args:
            spark: Spark session
            database_name: Database name
            table_name: Table name
            retention_hours: Hours to retain old files (default 7 days)
        """
        try:
            spark.sql(f"VACUUM {database_name}.{table_name} RETAIN {retention_hours} HOURS")
            logger.info(f"Table {database_name}.{table_name} vacuumed successfully")

        except Exception as e:
            logger.error(f"Error vacuuming table {database_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def get_cluster_info() -> Dict[str, Any]:
        """
        Get Databricks cluster information.

        Returns:
            Dict[str, Any]: Cluster information
        """
        try:
            # Try to get cluster information from Spark conf
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()

            if spark:
                cluster_info = {
                    "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown"),
                    "cluster_name": spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"),
                    "spark_version": spark.version,
                    "driver_node_type": spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "unknown"),
                    "worker_node_type": spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkerNodeType", "unknown"),
                    "num_workers": spark.conf.get("spark.databricks.clusterUsageTags.clusterMinWorkers", "unknown")
                }
            else:
                cluster_info = {"error": "No active Spark session"}

            return cluster_info

        except Exception as e:
            logger.error(f"Error getting cluster info: {str(e)}")
            return {"error": str(e)}

    @staticmethod
    def configure_spark_for_streaming(spark: SparkSession,
                                    max_files_per_trigger: int = 1000,
                                    checkpoint_location: str = None) -> None:
        """
        Configure Spark session for optimal streaming performance.

        Args:
            spark: Spark session
            max_files_per_trigger: Maximum files to process per trigger
            checkpoint_location: Checkpoint location for streaming
        """
        try:
            # Set streaming configurations
            spark.conf.set("spark.sql.streaming.maxFilesPerTrigger", str(max_files_per_trigger))
            spark.conf.set("spark.sql.streaming.minBatchesToRetain", "10")
            spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")

            # Set Delta configurations for streaming
            spark.conf.set("spark.databricks.delta.preview.enabled", "true")
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

            if checkpoint_location:
                spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_location)

            logger.info("Spark configured for streaming")

        except Exception as e:
            logger.error(f"Error configuring Spark for streaming: {str(e)}")
            raise

    @staticmethod
    def get_widget_values() -> Dict[str, str]:
        """
        Get all widget values from Databricks notebook.
        Used for debugging and logging configuration.

        Returns:
            Dict[str, str]: Dictionary of widget names and values
        """
        try:
            # This will work in Databricks environment
            widget_values = {}

            # List of expected widgets for Purchase Order Item pipeline
            widget_names = [
                "eventhub_scope", "eventhub_name", "batch_size", "send_interval",
                "duration_minutes", "log_level", "consumer_group", "bronze_path",
                "checkpoint_path", "storage_account", "container", "database_name",
                "table_name", "trigger_mode", "max_events_per_trigger",
                "quality_criticality", "quality_threshold"
            ]

            for widget_name in widget_names:
                try:
                    # In actual Databricks environment, dbutils.widgets.get() would be used
                    # For testing, we'll use a placeholder
                    widget_values[widget_name] = f"widget_{widget_name}_value"
                except:
                    widget_values[widget_name] = "not_set"

            return widget_values

        except Exception as e:
            logger.error(f"Error getting widget values: {str(e)}")
            return {}

    @staticmethod
    def log_dataframe_info(df: DataFrame, name: str = "DataFrame") -> None:
        """
        Log information about a DataFrame for debugging.

        Args:
            df: DataFrame to analyze
            name: Name for logging
        """
        try:
            logger.info(f"{name} Info:")
            logger.info(f"  Schema: {df.schema}")
            logger.info(f"  Column count: {len(df.columns)}")
            logger.info(f"  Columns: {df.columns}")

            # Try to get count (might be expensive for large DataFrames)
            try:
                count = df.count()
                logger.info(f"  Row count: {count}")
            except:
                logger.info("  Row count: Unable to determine (DataFrame might be streaming)")

        except Exception as e:
            logger.error(f"Error logging DataFrame info for {name}: {str(e)}")

class DatabricksNotebookHelper:
    """
    Helper class for common Databricks notebook operations.
    """

    @staticmethod
    def setup_logging(log_level: str = "INFO") -> None:
        """
        Set up logging for Databricks notebook.

        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Set specific loggers
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

    @staticmethod
    def display_widget_configuration() -> None:
        """
        Display current widget configuration in notebook.
        """
        try:
            widget_values = DatabricksUtils.get_widget_values()

            print("=== Purchase Order Item Pipeline Configuration ===")
            for key, value in widget_values.items():
                print(f"{key}: {value}")
            print("=" * 50)

        except Exception as e:
            print(f"Error displaying widget configuration: {str(e)}")

    @staticmethod
    def validate_environment() -> Dict[str, bool]:
        """
        Validate that the Databricks environment is properly configured.

        Returns:
            Dict[str, bool]: Validation results
        """
        validation_results = {}

        try:
            # Check Spark session
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            validation_results["spark_session"] = spark is not None

            # Check if running in Databricks
            try:
                # This would be available in actual Databricks environment
                validation_results["databricks_environment"] = True  # Placeholder
            except:
                validation_results["databricks_environment"] = False

            # Check Delta support
            try:
                if spark:
                    spark.sql("CREATE TABLE IF NOT EXISTS temp_delta_test (id INT) USING DELTA")
                    spark.sql("DROP TABLE IF EXISTS temp_delta_test")
                    validation_results["delta_support"] = True
                else:
                    validation_results["delta_support"] = False
            except:
                validation_results["delta_support"] = False

        except Exception as e:
            logger.error(f"Error validating environment: {str(e)}")
            validation_results["error"] = str(e)

        return validation_results

    @staticmethod
    def get_purchase_order_bronze_schema() -> StructType:
        """
        Get the Bronze layer schema for Purchase Order Item data.

        This schema includes:
        - Technical metadata (record_id, ingestion timestamps)
        - EventHub metadata (partition, offset, sequence number, enqueued time)
        - Processing metadata (cluster_id, consumer_group, eventhub_name)
        - Raw payload data (JSON string and payload size)

        Returns:
            StructType: Bronze layer schema for Purchase Order Items
        """
        return StructType([
            # Technical metadata fields
            StructField("record_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", StringType(), False),
            StructField("ingestion_hour", IntegerType(), False),

            # EventHub metadata fields
            StructField("eventhub_partition", IntegerType(), True),
            StructField("eventhub_offset", LongType(), True),
            StructField("eventhub_sequence_number", LongType(), True),
            StructField("eventhub_enqueued_time", TimestampType(), True),

            # Processing metadata
            StructField("processing_cluster_id", StringType(), True),
            StructField("consumer_group", StringType(), False),
            StructField("eventhub_name", StringType(), False),

            # Raw data fields
            StructField("raw_payload", StringType(), False),
            StructField("payload_size_bytes", LongType(), False)
        ])

    @staticmethod
    def create_purchase_order_bronze_table(spark: SparkSession,
                                          database_name: str,
                                          table_name: str,
                                          storage_path: str) -> None:
        """
        Create Bronze layer table for Purchase Order Items with proper partitioning.

        Args:
            spark: Spark session
            database_name: Database name
            table_name: Table name
            storage_path: Storage path for the table
        """
        try:
            # Use the database
            spark.sql(f"USE {database_name}")

            # Get Bronze schema
            bronze_schema = DatabricksUtils.get_purchase_order_bronze_schema()

            # Build CREATE TABLE statement
            columns_ddl = []
            for field in bronze_schema.fields:
                nullable = "NULL" if field.nullable else "NOT NULL"

                if field.dataType == StringType():
                    col_type = "STRING"
                elif field.dataType == IntegerType():
                    col_type = "INT"
                elif field.dataType == LongType():
                    col_type = "LONG"
                elif field.dataType == TimestampType():
                    col_type = "TIMESTAMP"
                elif isinstance(field.dataType, DecimalType):
                    col_type = f"DECIMAL({field.dataType.precision},{field.dataType.scale})"
                elif field.dataType == BooleanType():
                    col_type = "BOOLEAN"
                else:
                    col_type = "STRING"

                columns_ddl.append(f"  {field.name} {col_type} {nullable}")

            columns_str = ",\n".join(columns_ddl)

            # Create Bronze table with partitioning by ingestion_date and ingestion_hour
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {columns_str}
            )
            USING DELTA
            LOCATION '{storage_path}'
            PARTITIONED BY (ingestion_date, ingestion_hour)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '7 days',
                'delta.deletedFileRetentionDuration' = '7 days'
            )
            """

            spark.sql(create_table_sql)
            logger.info(f"Bronze table {database_name}.{table_name} created successfully")

        except Exception as e:
            logger.error(f"Error creating Bronze table {database_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def transform_eventhub_to_bronze(df: DataFrame,
                                   eventhub_name: str,
                                   consumer_group: str) -> DataFrame:
        """
        Transform EventHub streaming DataFrame to Bronze layer format.

        Args:
            df: Input EventHub streaming DataFrame
            eventhub_name: EventHub name
            consumer_group: Consumer group name

        Returns:
            DataFrame: Bronze layer formatted DataFrame
        """
        try:
            # Get cluster information
            cluster_info = DatabricksUtils.get_cluster_info()
            cluster_id = cluster_info.get('cluster_id', 'unknown')

            # Transform EventHub data to Bronze format
            bronze_df = df.select(
                # Generate unique record ID
                col("partitionId").cast("string").alias("eventhub_partition_str"),
                col("offset").cast("string").alias("offset_str"),
                col("sequenceNumber").cast("string").alias("sequence_str")
            ).withColumn(
                "record_id",
                concat_ws("_", lit("po"), col("eventhub_partition_str"), col("offset_str"), col("sequence_str"))
            ).withColumn(
                # Ingestion timestamps
                "ingestion_timestamp", current_timestamp()
            ).withColumn(
                "ingestion_date", date_format(col("ingestion_timestamp"), "yyyy-MM-dd")
            ).withColumn(
                "ingestion_hour", hour(col("ingestion_timestamp"))
            ).select(
                col("record_id"),
                col("ingestion_timestamp"),
                col("ingestion_date"),
                col("ingestion_hour"),

                # EventHub metadata
                col("partitionId").cast("int").alias("eventhub_partition"),
                col("offset").cast("long").alias("eventhub_offset"),
                col("sequenceNumber").cast("long").alias("eventhub_sequence_number"),
                col("enqueuedTime").alias("eventhub_enqueued_time"),

                # Processing metadata
                lit(cluster_id).alias("processing_cluster_id"),
                lit(consumer_group).alias("consumer_group"),
                lit(eventhub_name).alias("eventhub_name"),

                # Raw payload data
                col("body").cast("string").alias("raw_payload"),
                length(col("body")).alias("payload_size_bytes")
            )

            logger.info("EventHub data transformed to Bronze format successfully")
            return bronze_df

        except Exception as e:
            logger.error(f"Error transforming EventHub data to Bronze: {str(e)}")
            raise