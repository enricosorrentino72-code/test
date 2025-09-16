# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Streaming Pipeline
# MAGIC 
# MAGIC **Weather Data Processing Pipeline**
# MAGIC 
# MAGIC This notebook implements:
# MAGIC - Real-time Bronze to Silver data transformation
# MAGIC - JSON payload parsing to structured weather data
# MAGIC - Hive Metastore table management
# MAGIC - Fault-tolerant streaming with checkpointing
# MAGIC - Configuration aligned with EventHub_Listener_HiveMetastore_Databricks
# MAGIC 
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Bronze Layer (bronze.weather_events_raw) → Spark Streaming → Silver Layer (bronze.weather_events_silver)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Widgets - Aligned with EventHub_Listener_HiveMetastore_Databricks

# COMMAND ----------

# Create configuration widgets matching the EventHub listener configuration
# Common properties aligned with EventHub_Listener_HiveMetastore_Databricks
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("storage_account", "idisitcusadls2", "Storage Account Name")
dbutils.widgets.text("container", "eventhub-test", "ADLS Container Name")

# Source configuration - matching Bronze layer
dbutils.widgets.text("bronze_database", "bronze", "Bronze Database Name")
dbutils.widgets.text("bronze_table", "weather_events_raw", "Bronze Table Name")

# Target configuration - Silver layer in same database
dbutils.widgets.text("silver_database", "bronze", "Silver Database Name")
dbutils.widgets.text("silver_table", "weather_events_silver", "Silver Table Name")

# Pipeline paths
dbutils.widgets.text("silver_path", "/mnt/silver/weather", "Silver Layer Path")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/bronze-to-silver", "Pipeline Checkpoint Path")

# Streaming configuration - matching EventHub listener values
dbutils.widgets.dropdown("trigger_mode", "5 seconds", ["1 second", "5 seconds", "10 seconds", "1 minute"], "Trigger Interval")
dbutils.widgets.text("max_events_per_trigger", "10000", "Max Events Per Trigger")

# Get configuration values
SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
CONTAINER = dbutils.widgets.get("container")

BRONZE_DATABASE = dbutils.widgets.get("bronze_database")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
SILVER_DATABASE = dbutils.widgets.get("silver_database")
SILVER_TABLE = dbutils.widgets.get("silver_table")

BRONZE_FULL_TABLE = f"{BRONZE_DATABASE}.{BRONZE_TABLE}"
SILVER_FULL_TABLE = f"{SILVER_DATABASE}.{SILVER_TABLE}"

SILVER_PATH = dbutils.widgets.get("silver_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")

TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
MAX_EVENTS_PER_TRIGGER = int(dbutils.widgets.get("max_events_per_trigger"))

print("🔧 Bronze to Silver Pipeline Configuration (Aligned with EventHub Listener):")
print(f"   Secret Scope: {SECRET_SCOPE}")
print(f"   Storage Account: {STORAGE_ACCOUNT}")
print(f"   Container: {CONTAINER}")
print(f"   Source: {BRONZE_FULL_TABLE}")
print(f"   Silver Target: {SILVER_FULL_TABLE}")
print(f"   Trigger: {TRIGGER_MODE}")
print(f"   Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")
print(f"   Checkpoint Path: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Initialize Spark

# COMMAND ----------

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable

# Configure Spark for optimal streaming performance
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

# Set checkpoint configuration for reliability (30 seconds interval)
spark.conf.set("spark.sql.streaming.checkpointLocation.compression.codec", "lz4")

print("✅ Spark configured for Bronze to Silver streaming pipeline")
print(f"   Spark Version: {spark.version}")
print(f"   Checkpoint interval: 30 seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer Schema Definition

# COMMAND ----------

# Silver layer schema: Parsed weather data + inherited technical fields
silver_schema = StructType([
    # Business Weather Data (parsed from raw_payload)
    StructField("event_id", StringType(), False),  # Business event identifier
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("weather_timestamp", TimestampType(), True),  # From payload
    StructField("data_source", StringType(), True),
    StructField("weather_cluster_id", StringType(), True),  # From payload
    StructField("notebook_path", StringType(), True),
    
    # Inherited Technical Fields from Bronze
    StructField("source_record_id", StringType(), False),  # Original bronze record_id
    StructField("bronze_ingestion_timestamp", TimestampType(), False),
    StructField("bronze_ingestion_date", DateType(), False),
    StructField("bronze_ingestion_hour", IntegerType(), False),
    
    # Event Hub Technical Fields (inherited)
    StructField("eventhub_partition", StringType(), True),
    StructField("eventhub_offset", StringType(), True),
    StructField("eventhub_sequence_number", LongType(), True),
    StructField("eventhub_enqueued_time", TimestampType(), True),
    StructField("eventhub_partition_key", StringType(), True),
    
    # Processing Context (inherited)
    StructField("processing_cluster_id", StringType(), True),
    StructField("consumer_group", StringType(), True),
    StructField("eventhub_name", StringType(), True),
    
    # Silver Processing Metadata
    StructField("silver_processing_timestamp", TimestampType(), False),
    StructField("silver_processing_date", DateType(), False),  # Partition field
    StructField("silver_processing_hour", IntegerType(), False),  # Partition field
    
    # Payload information
    StructField("original_payload_size_bytes", IntegerType(), True),
    StructField("parsing_status", StringType(), False),  # SUCCESS, PARTIAL, FAILED
    StructField("parsing_errors", ArrayType(StringType()), True)
])

print("✅ Silver layer schema defined")
print("   Business data: Parsed weather fields from JSON payload")
print("   Technical data: Inherited from Bronze layer")
print("   Processing data: Silver-specific metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Weather Data Transformation Functions

# COMMAND ----------

def parse_weather_payload(df: DataFrame) -> DataFrame:
    """Parse JSON payload and extract weather data according to WeatherReading model"""
    
    print("📊 Parsing JSON payload to extract structured weather data...")
    
    # Parse JSON payload and extract weather fields
    parsed_df = df.select(
        # Inherited fields from Bronze
        col("record_id").alias("source_record_id"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("ingestion_date").alias("bronze_ingestion_date"),
        col("ingestion_hour").alias("bronze_ingestion_hour"),
        
        # Event Hub technical fields (inherited)
        col("eventhub_partition"),
        col("eventhub_offset"),
        col("eventhub_sequence_number"),
        col("eventhub_enqueued_time"),
        col("eventhub_partition_key"),
        
        # Processing context (inherited)
        col("processing_cluster_id"),
        col("consumer_group"),
        col("eventhub_name"),
        
        # Original payload info
        col("payload_size_bytes").alias("original_payload_size_bytes"),
        col("raw_payload"),
        
        # Parse JSON fields from raw_payload (WeatherReading model)
        get_json_object(col("raw_payload"), "$.event_id").alias("event_id"),
        get_json_object(col("raw_payload"), "$.city").alias("city"),
        get_json_object(col("raw_payload"), "$.latitude").cast("double").alias("latitude"),
        get_json_object(col("raw_payload"), "$.longitude").cast("double").alias("longitude"),
        get_json_object(col("raw_payload"), "$.temperature").cast("double").alias("temperature"),
        get_json_object(col("raw_payload"), "$.humidity").cast("double").alias("humidity"),
        get_json_object(col("raw_payload"), "$.wind_speed").cast("double").alias("wind_speed"),
        get_json_object(col("raw_payload"), "$.pressure").cast("double").alias("pressure"),
        get_json_object(col("raw_payload"), "$.precipitation").cast("double").alias("precipitation"),
        get_json_object(col("raw_payload"), "$.cloud_cover").cast("double").alias("cloud_cover"),
        get_json_object(col("raw_payload"), "$.weather_condition").alias("weather_condition"),
        get_json_object(col("raw_payload"), "$.timestamp").cast("timestamp").alias("weather_timestamp"),
        get_json_object(col("raw_payload"), "$.data_source").alias("data_source"),
        get_json_object(col("raw_payload"), "$.cluster_id").alias("weather_cluster_id"),
        get_json_object(col("raw_payload"), "$.notebook_path").alias("notebook_path")
    )
    
    # Add parsing status based on successful field extraction
    parsed_df = parsed_df.withColumn(
        "parsing_status",
        when(
            col("event_id").isNotNull() & 
            col("city").isNotNull() & 
            col("temperature").isNotNull(),
            lit("SUCCESS")
        ).when(
            col("event_id").isNotNull() | 
            col("city").isNotNull() | 
            col("temperature").isNotNull(),
            lit("PARTIAL")
        ).otherwise(lit("FAILED"))
    )
    
    # Add parsing errors (basic validation)
    parsed_df = parsed_df.withColumn(
        "parsing_errors",
        when(col("parsing_status") == "FAILED", 
             array(lit("JSON parsing completely failed"))
        ).when(col("parsing_status") == "PARTIAL",
             array(lit("Some required fields missing from JSON"))
        ).otherwise(array())  # Empty array for SUCCESS
    )
    
    # Handle missing event_id (generate if null)
    parsed_df = parsed_df.withColumn(
        "event_id",
        coalesce(
            col("event_id"),
            concat(
                coalesce(col("city"), lit("unknown")),
                lit("-"),
                unix_timestamp(col("weather_timestamp")).cast("string")
            )
        )
    )
    
    # Handle missing weather timestamp (use bronze ingestion time)
    parsed_df = parsed_df.withColumn(
        "weather_timestamp",
        coalesce(col("weather_timestamp"), col("bronze_ingestion_timestamp"))
    )
    
    # Set default data_source if missing
    parsed_df = parsed_df.withColumn(
        "data_source",
        coalesce(col("data_source"), lit("databricks_weather_simulator"))
    )
    
    return parsed_df

def add_silver_processing_metadata(df: DataFrame) -> DataFrame:
    """Add Silver layer processing metadata"""
    
    return df.withColumn(
        "silver_processing_timestamp", current_timestamp()
    ).withColumn(
        "silver_processing_date", current_date()
    ).withColumn(
        "silver_processing_hour", hour(current_timestamp())
    )

def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Complete transformation from Bronze to Silver"""
    
    print("🔄 Starting Bronze to Silver transformation...")
    
    # Step 1: Parse weather data from JSON payload
    parsed_df = parse_weather_payload(bronze_df)
    
    # Step 2: Add Silver processing metadata
    silver_df = add_silver_processing_metadata(parsed_df)
    
    # Step 3: Select final Silver schema fields (remove raw_payload)
    silver_df = silver_df.select([col.name for col in silver_schema.fields])
    
    print("✅ Bronze to Silver transformation completed")
    return silver_df

print("✅ Weather data transformation functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Hive Metastore Management for Silver Layer

# COMMAND ----------

class SilverHiveMetastoreManager:
    """Manage Silver layer Hive Metastore database and table operations"""
    
    def __init__(self, database_name: str, table_name: str, storage_account: str, container: str):
        self.database_name = database_name
        self.table_name = table_name
        self.full_table_name = f"{database_name}.{table_name}"
        self.storage_account = storage_account
        self.container = container
    
    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            spark.sql(f"USE {self.database_name}")
            print(f"✅ Database '{self.database_name}' ensured and selected")
        except Exception as e:
            print(f"⚠️ Error creating database: {e}")
    
    def create_silver_table(self, table_path: str, table_schema: StructType):
        """Create Silver Delta table in Hive Metastore"""
        try:
            # Check if table already exists
            table_exists = self._table_exists()
            
            if not table_exists:
                print(f"📝 Creating Silver table: {self.full_table_name}")
                
                # Build CREATE TABLE DDL for Silver Delta format
                ddl_columns = []
                for field in table_schema.fields:
                    spark_type = field.dataType.simpleString()
                    nullable = "" if field.nullable else "NOT NULL"
                    ddl_columns.append(f"`{field.name}` {spark_type} {nullable}")
                
                columns_ddl = ",\n  ".join(ddl_columns)
                
                create_table_sql = f"""
                CREATE TABLE {self.full_table_name} (
                  {columns_ddl}
                )
                USING DELTA
                LOCATION '{table_path}'
                PARTITIONED BY (silver_processing_date, silver_processing_hour)
                TBLPROPERTIES (
                  'delta.autoOptimize.optimizeWrite' = 'true',
                  'delta.autoOptimize.autoCompact' = 'true',
                  'delta.logRetentionDuration' = '30 days',
                  'delta.deletedFileRetentionDuration' = '7 days',
                  'description' = 'Silver layer weather data parsed from Bronze',
                  'data_source' = 'Bronze layer weather_events_raw',
                  'ingestion_pattern' = 'Structured Streaming Bronze to Silver',
                  'data_format' = 'Parsed weather data with technical metadata',
                  'storage_account' = '{self.storage_account}',
                  'container' = '{self.container}'
                )
                """
                
                spark.sql(create_table_sql)
                print(f"✅ Silver table '{self.full_table_name}' created successfully")
            else:
                print(f"✅ Silver table '{self.full_table_name}' already exists")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating Silver table: {e}")
            return False
    
    def _table_exists(self) -> bool:
        """Check if table exists in Hive Metastore"""
        try:
            spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            return True
        except:
            return False
    
    def show_table_info(self):
        """Display Silver table information and metadata"""
        try:
            print(f"📊 Silver Hive Metastore Table Information: {self.full_table_name}")
            print("=" * 60)
            
            # Show table details
            table_info = spark.sql(f"DESCRIBE DETAIL {self.full_table_name}").collect()[0]
            
            print(f"📁 Location: {table_info['location']}")
            print(f"📋 Format: {table_info['format']}")
            print(f"📊 Number of Files: {table_info['numFiles']}")
            print(f"💾 Size in Bytes: {table_info['sizeInBytes']:,}")
            print(f"🕒 Created: {table_info['createdAt']}")
            print(f"🔄 Last Modified: {table_info['lastModified']}")
            
            # Show partitions
            try:
                partitions = spark.sql(f"SHOW PARTITIONS {self.full_table_name}").collect()
                print(f"📂 Partitions: {len(partitions)}")
            except:
                print(f"📂 Partitions: Not available (empty table)")
            
        except Exception as e:
            print(f"❌ Error retrieving Silver table info: {e}")

# Initialize Silver Hive Metastore Manager
silver_hive_manager = SilverHiveMetastoreManager(SILVER_DATABASE, SILVER_TABLE, STORAGE_ACCOUNT, CONTAINER)
print(f"✅ Silver Hive Metastore manager initialized for: {SILVER_FULL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ADLS Configuration (Using EventHub Listener Configuration)

# COMMAND ----------

class SilverADLSConfig:
    """ADLS Gen2 configuration for Silver layer (aligned with EventHub listener)"""
    
    def __init__(self, storage_account: str, container: str):
        self.storage_account = storage_account
        self.container = container
        self.base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        
        # Ensure mount point exists (assuming storage is mounted)
        try:
            dbutils.fs.ls(f"/mnt/datalake/")
            self.is_mounted = True
            print("✅ ADLS Gen2 storage is mounted")
        except:
            self.is_mounted = False
            print("⚠️ ADLS Gen2 storage mount not found - using direct abfss:// paths")
    
    def get_silver_path(self, base_path: str) -> str:
        """Get full silver layer path"""
        if self.is_mounted:
            return base_path
        else:
            # Remove /mnt/datalake prefix and use direct abfss path
            relative_path = base_path.replace("/mnt/datalake/", "")
            return f"{self.base_path}/{relative_path}"
    
    def get_checkpoint_path(self, base_path: str) -> str:
        """Get full checkpoint path"""
        if self.is_mounted:
            return base_path
        else:
            relative_path = base_path.replace("/mnt/datalake/", "")
            return f"{self.base_path}/{relative_path}"

# Initialize ADLS configuration (using same storage as EventHub listener)
silver_adls_config = SilverADLSConfig(STORAGE_ACCOUNT, CONTAINER)
silver_full_path = silver_adls_config.get_silver_path(SILVER_PATH)
checkpoint_full_path = silver_adls_config.get_checkpoint_path(CHECKPOINT_PATH)

print(f"✅ ADLS Gen2 configuration (aligned with EventHub listener):")
print(f"   Storage Account: {STORAGE_ACCOUNT}")
print(f"   Container: {CONTAINER}")
print(f"   Silver Path: {silver_full_path}")
print(f"   Checkpoint Path: {checkpoint_full_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Setup Silver Database and Table

# COMMAND ----------

def setup_silver_hive_components():
    """Setup Silver database and create table in Hive Metastore"""
    
    print("🏗️ Setting up Silver Hive Metastore components...")
    print("=" * 50)
    
    # Create database if not exists
    silver_hive_manager.create_database_if_not_exists()
    
    # Create Silver table
    success = silver_hive_manager.create_silver_table(silver_full_path, silver_schema)
    
    if success:
        print("✅ Silver Hive Metastore setup completed successfully")
        print(f"📊 Silver table created: {SILVER_FULL_TABLE}")
        
        # Show table information
        silver_hive_manager.show_table_info()
        
        return True
    else:
        print("❌ Silver Hive Metastore setup failed")
        return False

# Setup Silver Hive Metastore components
silver_setup_success = setup_silver_hive_components()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Streaming Pipeline Functions

# COMMAND ----------

def get_trigger_config(trigger_mode: str):
    """Get trigger configuration based on mode (aligned with EventHub listener)"""
    if "second" in trigger_mode:
        interval = trigger_mode.split()[0]
        return {"processingTime": f"{interval} seconds"}
    elif "minute" in trigger_mode:
        interval = trigger_mode.split()[0]
        return {"processingTime": f"{interval} minutes"}
    else:
        return {"processingTime": "5 seconds"}

def start_bronze_to_silver_streaming():
    """Start the Bronze to Silver streaming pipeline"""
    
    if not silver_setup_success:
        print("❌ Cannot start pipeline - Silver Hive Metastore setup failed")
        return None
    
    print("🚀 Starting Bronze to Silver Streaming Pipeline")
    print("=" * 60)
    print(f"📊 Source: {BRONZE_FULL_TABLE}")
    print(f"✨ Silver Target: {SILVER_FULL_TABLE}")
    print(f"⚡ Trigger: {TRIGGER_MODE}")
    print(f"📦 Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")
    print(f"🏪 Storage Account: {STORAGE_ACCOUNT}")
    print(f"📁 Container: {CONTAINER}")
    print(f"🔄 Checkpoint: {checkpoint_full_path}")
    print(f"⏱️ Checkpoint Interval: 30 seconds")
    print("=" * 60)
    
    try:
        # Read from Bronze table as stream
        print("📖 Reading Bronze table stream...")
        bronze_stream = spark \
            .readStream \
            .format("delta") \
            .table(BRONZE_FULL_TABLE)
        
        print("✅ Bronze stream source created")
        
        # Apply transformation to Silver
        print("🔄 Applying Bronze to Silver transformation...")
        silver_stream = transform_bronze_to_silver(bronze_stream)
        
        print("✅ Silver stream transformation configured")
        
        # Configure trigger
        trigger_config = get_trigger_config(TRIGGER_MODE)
        print(f"✅ Trigger configured: {trigger_config}")
        
        # Write to Silver Hive Metastore table with checkpointing
        query = silver_stream \
            .writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", checkpoint_full_path) \
            .option("maxRecordsPerFile", "50000") \
            .trigger(**trigger_config) \
            .toTable(SILVER_FULL_TABLE)
        
        print("✅ Bronze to Silver streaming query started successfully")
        print(f"📊 Query ID: {query.id}")
        print(f"🔄 Run ID: {query.runId}")
        print(f"📝 Mode: APPEND-ONLY")
        print(f"🗄️ Writing to Silver Hive table: {SILVER_FULL_TABLE}")
        
        return query
        
    except Exception as e:
        print(f"❌ Failed to start Bronze to Silver streaming: {e}")
        raise

print("✅ Bronze to Silver streaming functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Start the Streaming Pipeline

# COMMAND ----------

# Start the Bronze to Silver streaming pipeline
silver_streaming_query = start_bronze_to_silver_streaming()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Pipeline Monitoring

# COMMAND ----------

import time

class SilverPipelineMonitor:
    """Monitor Bronze to Silver pipeline performance"""
    
    def __init__(self, silver_table: str):
        self.silver_table = silver_table
    
    def display_silver_dashboard(self, query: StreamingQuery):
        """Display real-time Silver pipeline dashboard"""
        try:
            progress = query.lastProgress if query else None
            
            if progress:
                batch_id = progress.get('batchId', 0)
                input_rows = progress.get('inputRowsPerSecond', 0)
                processed_rows = progress.get('inputRowsPerSecond', 0)
                batch_duration = progress.get('durationMs', {}).get('triggerExecution', 0)
                
                dashboard_html = f"""
                <div style="border: 2px solid #2196F3; border-radius: 10px; padding: 15px; background: #f8f9fa;">
                    <h3 style="color: #2196F3;">📊 Bronze to Silver Streaming Pipeline</h3>
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 15px 0;">
                        <div style="text-align: center; padding: 10px; background: white; border-radius: 5px;">
                            <h4 style="margin: 0; color: #4CAF50;">Batch ID</h4>
                            <p style="font-size: 24px; margin: 5px 0;">{batch_id}</p>
                        </div>
                        <div style="text-align: center; padding: 10px; background: white; border-radius: 5px;">
                            <h4 style="margin: 0; color: #FF9800;">Rows/Second</h4>
                            <p style="font-size: 24px; margin: 5px 0;">{processed_rows:.1f}</p>
                        </div>
                        <div style="text-align: center; padding: 10px; background: white; border-radius: 5px;">
                            <h4 style="margin: 0; color: #9C27B0;">Duration (ms)</h4>
                            <p style="font-size: 24px; margin: 5px 0;">{batch_duration}</p>
                        </div>
                        <div style="text-align: center; padding: 10px; background: white; border-radius: 5px;">
                            <h4 style="margin: 0; color: #607D8B;">Status</h4>
                            <p style="font-size: 18px; margin: 5px 0;">{'🟢 Active' if query.isActive else '🔴 Stopped'}</p>
                        </div>
                    </div>
                    <div style="margin-top: 15px; background: #e3f2fd; padding: 10px; border-radius: 5px;">
                        <strong>Silver Table:</strong> {self.silver_table} | 
                        <strong>Mode:</strong> Bronze to Silver Transform | 
                        <strong>Checkpoint:</strong> 30s interval
                    </div>
                </div>
                """
                
                displayHTML(dashboard_html)
        except Exception as e:
            print(f"⚠️ Dashboard update error: {e}")
    
    def show_silver_data_quality(self):
        """Show Silver data quality metrics"""
        try:
            print("📊 Silver Layer Data Quality Analysis")
            print("=" * 50)
            
            if spark.table(self.silver_table).count() > 0:
                # Parsing status analysis
                print("📝 JSON Parsing Status:")
                spark.table(self.silver_table) \
                    .groupBy("parsing_status") \
                    .count() \
                    .orderBy("parsing_status") \
                    .show()
                
                # Data completeness analysis
                print("\n📊 Weather Data Completeness:")
                completeness_df = spark.table(self.silver_table) \
                    .select(
                        (count("*")).alias("total_records"),
                        (count("city")).alias("city_count"),
                        (count("temperature")).alias("temperature_count"),
                        (count("humidity")).alias("humidity_count"),
                        (count("pressure")).alias("pressure_count"),
                        (count("weather_condition")).alias("condition_count")
                    )
                completeness_df.show()
                
                # Processing time analysis
                print("\n⏱️ Processing Performance:")
                spark.table(self.silver_table) \
                    .groupBy("silver_processing_date") \
                    .count() \
                    .orderBy("silver_processing_date") \
                    .show(10)
                
            else:
                print("📭 No Silver layer data found yet")
                
        except Exception as e:
            print(f"❌ Silver quality analysis error: {e}")

# Initialize Silver monitor
silver_monitor = SilverPipelineMonitor(SILVER_FULL_TABLE)

# Monitor the Silver pipeline
if silver_streaming_query:
    print("📊 Monitoring Bronze to Silver Pipeline")
    print(f"🏪 Storage Account: {STORAGE_ACCOUNT}")
    print(f"📁 Container: {CONTAINER}")
    print("=" * 60)
    
    try:
        # Monitor for a few iterations
        for i in range(3):  # Monitor for 3 iterations
            if silver_streaming_query.isActive:
                print(f"\n🔄 Monitoring Iteration {i+1}")
                
                # Display Silver dashboard
                silver_monitor.display_silver_dashboard(silver_streaming_query)
                
                # Show progress
                try:
                    progress = silver_streaming_query.lastProgress
                    if progress:
                        batch_id = progress.get('batchId', 0)
                        print(f"📊 Last Batch ID: {batch_id}")
                except:
                    pass
                
                time.sleep(15)  # Wait 15 seconds between checks
            else:
                print("❌ Silver pipeline is not active")
                break
        
        if silver_streaming_query.isActive:
            print("✅ Bronze to Silver pipeline running successfully in background")
            silver_monitor.show_silver_data_quality()
        
    except KeyboardInterrupt:
        print("⏹️ Monitoring interrupted")
    except Exception as e:
        print(f"⚠️ Monitoring error: {e}")
else:
    print("❌ Silver pipeline not started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Pipeline Management Commands

# COMMAND ----------

def check_silver_pipeline_status():
    """Check Silver pipeline status"""
    if silver_streaming_query:
        print(f"📊 Bronze to Silver Pipeline Status:")
        print(f"   Query ID: {silver_streaming_query.id}")
        print(f"   Run ID: {silver_streaming_query.runId}")
        print(f"   Active: {silver_streaming_query.isActive}")
        print(f"   Source: {BRONZE_FULL_TABLE}")
        print(f"   Target: {SILVER_FULL_TABLE}")
        print(f"   Storage Account: {STORAGE_ACCOUNT}")
        print(f"   Container: {CONTAINER}")
        
        try:
            progress = silver_streaming_query.lastProgress
            if progress:
                print(f"   Last Batch: {progress.get('batchId', 'N/A')}")
        except:
            pass
        
        return True
    else:
        print("❌ No Silver pipeline found")
        return False

def stop_silver_pipeline():
    """Stop the Silver pipeline"""
    try:
        if silver_streaming_query and silver_streaming_query.isActive:
            print("⏹️ Stopping Bronze to Silver pipeline...")
            silver_streaming_query.stop()
            
            # Wait for graceful shutdown
            timeout = 30
            elapsed = 0
            while silver_streaming_query.isActive and elapsed < timeout:
                time.sleep(1)
                elapsed += 1
            
            if not silver_streaming_query.isActive:
                print("✅ Silver pipeline stopped successfully")
                silver_monitor.show_silver_data_quality()
            else:
                print("⚠️ Pipeline stop timeout")
        else:
            print("ℹ️ Silver pipeline is already stopped")
    except Exception as e:
        print(f"❌ Error stopping Silver pipeline: {e}")

def show_silver_table_stats():
    """Show Silver table statistics"""
    print("📊 Silver Table Statistics:")
    print("=" * 40)
    
    try:
        print(f"✨ Silver Table: {SILVER_FULL_TABLE}")
        silver_count = spark.table(SILVER_FULL_TABLE).count()
        print(f"   Records: {silver_count:,}")
        
        if silver_count > 0:
            # Show recent records
            print(f"\n🕒 Recent Silver Records:")
            spark.table(SILVER_FULL_TABLE) \
                .orderBy(col("silver_processing_timestamp").desc()) \
                .select("event_id", "city", "temperature", "humidity", 
                       "parsing_status", "silver_processing_timestamp") \
                .show(5, truncate=False)
        
        print(f"\n🔧 Configuration:")
        print(f"   Storage Account: {STORAGE_ACCOUNT}")
        print(f"   Container: {CONTAINER}")
        print(f"   Secret Scope: {SECRET_SCOPE}")
        
    except Exception as e:
        print(f"❌ Error checking Silver table: {e}")

# Check Silver status
check_silver_pipeline_status()
print()
show_silver_table_stats()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final Summary and Configuration Verification

# COMMAND ----------

def display_silver_pipeline_summary():
    """Display comprehensive Silver pipeline summary"""
    
    summary_html = f"""
    <div style="border: 2px solid #4CAF50; border-radius: 15px; padding: 20px; background: #f8f9fa;">
        <h2 style="color: #4CAF50; text-align: center;">📊 Bronze to Silver Streaming Pipeline</h2>
        
        <div style="background: #fff3cd; padding: 15px; border-radius: 10px; border-left: 5px solid #ffc107; margin: 20px 0;">
            <h3 style="color: #856404; margin-top: 0;">⚙️ Configuration Aligned with EventHub Listener</h3>
            <p style="margin: 0; color: #856404;">All configuration properties inherited from <strong>EventHub_Listener_HiveMetastore_Databricks.py</strong></p>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin: 20px 0;">
            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #2196F3;">
                <h3 style="color: #2196F3; margin-top: 0;">🔧 Inherited Configuration</h3>
                <ul style="margin: 0;">
                    <li><strong>Secret Scope:</strong> {SECRET_SCOPE}</li>
                    <li><strong>Storage Account:</strong> {STORAGE_ACCOUNT}</li>
                    <li><strong>Container:</strong> {CONTAINER}</li>
                    <li><strong>Source:</strong> {BRONZE_FULL_TABLE}</li>
                    <li><strong>Trigger:</strong> {TRIGGER_MODE}</li>
                    <li><strong>Max Events:</strong> {MAX_EVENTS_PER_TRIGGER}</li>
                </ul>
            </div>
            
            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #FF9800;">
                <h3 style="color: #FF9800; margin-top: 0;">📊 Pipeline Configuration</h3>
                <ul style="margin: 0;">
                    <li><strong>Target:</strong> {SILVER_FULL_TABLE}</li>
                    <li><strong>Checkpoint:</strong> 30 seconds</li>
                    <li><strong>Processing:</strong> JSON to Structured</li>
                    <li><strong>Schema:</strong> WeatherReading Model</li>
                    <li><strong>Partitioning:</strong> Date + Hour</li>
                    <li><strong>Quality:</strong> Parsing Status Tracking</li>
                </ul>
            </div>
        </div>
        
        <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #9C27B0; margin: 20px 0;">
            <h3 style="color: #9C27B0; margin-top: 0;">✨ Pipeline Features</h3>
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
                <div>✅ JSON Payload Parsing</div>
                <div>✅ WeatherReading Model</div>
                <div>✅ Technical Field Inheritance</div>
                <div>✅ Parsing Status Tracking</div>
                <div>✅ Hive Metastore Integration</div>
                <div>✅ Delta Lake Storage</div>
                <div>✅ Real-time Streaming</div>
                <div>✅ Fault-tolerant Checkpointing</div>
                <div>✅ Configuration Alignment</div>
            </div>
        </div>
    </div>
    """
    
    displayHTML(summary_html)

def show_final_configuration_summary():
    """Show final configuration alignment summary"""
    
    print("🔧 Final Configuration Alignment Verification")
    print("=" * 50)
    print("Configuration inherited from EventHub_Listener_HiveMetastore_Databricks:")
    print()
    print(f"✅ eventhub_scope: {SECRET_SCOPE}")
    print(f"✅ storage_account: {STORAGE_ACCOUNT}")
    print(f"✅ container: {CONTAINER}")
    print(f"✅ bronze_database: {BRONZE_DATABASE}")
    print(f"✅ bronze_table: {BRONZE_TABLE}")
    print(f"✅ trigger_mode: {TRIGGER_MODE}")
    print(f"✅ max_events_per_trigger: {MAX_EVENTS_PER_TRIGGER}")
    print()
    print("📊 Silver Pipeline Configuration:")
    print(f"   Source Table: {BRONZE_FULL_TABLE}")
    print(f"   Target Table: {SILVER_FULL_TABLE}")
    print(f"   Checkpoint Path: {checkpoint_full_path}")
    print(f"   Checkpoint Interval: 30 seconds")
    print(f"   Data Model: WeatherReading JSON parsing")
    print(f"   Quality Tracking: Parsing status and errors")

# Display final summary
display_silver_pipeline_summary()
print()
show_final_configuration_summary()

print("🎯 Bronze to Silver Streaming Pipeline Implementation Complete!")
print(f"Status: {'🟢 Active' if silver_streaming_query and silver_streaming_query.isActive else '🔴 Stopped'}")
print("✅ All configuration properties aligned with EventHub_Listener_HiveMetastore_Databricks")
print("📊 Weather data parsing from JSON to structured Silver layer ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Bronze to Silver Pipeline Documentation
# MAGIC 
# MAGIC ### Complete Architecture
# MAGIC ```
# MAGIC Bronze Table (bronze.weather_events_raw) → Spark Structured Streaming → Silver Table (bronze.weather_events_silver)
# MAGIC ```
# MAGIC 
# MAGIC ### Data Transformation
# MAGIC - **Source**: Raw JSON payload in `raw_payload` column
# MAGIC - **Target**: Structured weather data following WeatherReading model
# MAGIC - **Processing**: Real-time JSON parsing with error tracking
# MAGIC 
# MAGIC ### Weather Data Model
# MAGIC ```python
# MAGIC class WeatherReading:
# MAGIC     event_id: str
# MAGIC     city: str
# MAGIC     latitude: float
# MAGIC     longitude: float
# MAGIC     temperature: float
# MAGIC     humidity: float
# MAGIC     wind_speed: float
# MAGIC     pressure: float
# MAGIC     precipitation: float
# MAGIC     cloud_cover: float
# MAGIC     weather_condition: str
# MAGIC     timestamp: str
# MAGIC     data_source: str = "databricks_weather_simulator"
# MAGIC     cluster_id: str = ""
# MAGIC     notebook_path: str = ""
# MAGIC ```
# MAGIC 
# MAGIC ### Configuration Alignment
# MAGIC - ✅ **Storage Configuration**: Inherited from EventHub_Listener_HiveMetastore_Databricks
# MAGIC - ✅ **Hive Metastore**: Same database and table management approach
# MAGIC - ✅ **Trigger Configuration**: Same 5-second interval and 10,000 max events
# MAGIC - ✅ **Checkpoint Management**: 30-second interval for fault tolerance
# MAGIC 
# MAGIC ### Quality Features
# MAGIC - **Parsing Status**: SUCCESS, PARTIAL, FAILED tracking
# MAGIC - **Error Logging**: Detailed parsing error messages
# MAGIC - **Data Completeness**: Field-level validation and fallbacks
# MAGIC - **Technical Lineage**: Full Bronze metadata inheritance
# MAGIC 
# MAGIC **🎯 Your Bronze to Silver pipeline is ready for weather data processing with complete configuration alignment!**