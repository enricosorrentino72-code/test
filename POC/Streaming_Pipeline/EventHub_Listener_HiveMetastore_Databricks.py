# Databricks notebook source
# MAGIC %md
# MAGIC # Event Hub Structured Streaming Listener with Hive Metastore
# MAGIC 
# MAGIC **Simplified Spark Structured Streaming job with Hive Metastore integration**
# MAGIC 
# MAGIC This notebook implements:
# MAGIC - Real-time Event Hub data consumption using Kafka API (eliminates Base64 issues)
# MAGIC - Simplified Bronze layer with technical fields + raw payload
# MAGIC - **Hive Metastore table registration** for data governance
# MAGIC - Append-only strategy for all records
# MAGIC - Checkpoint management for fault tolerance
# MAGIC - No data validation - raw data preservation
# MAGIC 
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Event Hub (Kafka API) → Spark Streaming → Bronze Layer → ADLS Gen2 → Hive Metastore Table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Widgets

# COMMAND ----------

# Create configuration widgets for dynamic parameters
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("eventhub_name", "weather-events", "Event Hub Name")
dbutils.widgets.text("consumer_group", "$Default", "Consumer Group")
dbutils.widgets.text("bronze_path", "/mnt/bronze/weather", "Bronze Layer Path (Local DBFS)")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/weather-listener", "Checkpoint Path (Local DBFS)")
dbutils.widgets.text("storage_account", "idisitcusadls2", "Storage Account Name")
dbutils.widgets.text("container", "eventhub-test", "ADLS Container Name")

# Hive Metastore table configuration (database.table format)
dbutils.widgets.text("database_name", "bronze", "Database Name")
dbutils.widgets.text("table_name", "weather_events_raw", "Table Name")

# Streaming configuration
dbutils.widgets.dropdown("trigger_mode", "5 seconds", ["1 second", "5 seconds", "10 seconds", "1 minute", "continuous"], "Trigger Interval")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")
dbutils.widgets.text("max_events_per_trigger", "10000", "Max Events Per Trigger")

# Get configuration values
SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CONSUMER_GROUP = dbutils.widgets.get("consumer_group")
BRONZE_PATH = dbutils.widgets.get("bronze_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
CONTAINER = dbutils.widgets.get("container")

# Hive Metastore configuration
DATABASE_NAME = dbutils.widgets.get("database_name")
TABLE_NAME = dbutils.widgets.get("table_name")
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"

# Streaming configuration
TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
LOG_LEVEL = dbutils.widgets.get("log_level")
MAX_EVENTS_PER_TRIGGER = int(dbutils.widgets.get("max_events_per_trigger"))

print("🔧 Configuration Loaded:")
print(f"   Event Hub: {EVENTHUB_NAME}")
print(f"   Consumer Group: {CONSUMER_GROUP}")
print(f"   Bronze Path: {BRONZE_PATH}")
print(f"   Checkpoint: {CHECKPOINT_PATH}")
print(f"   Hive Table: {FULL_TABLE_NAME}")
print(f"   Trigger: {TRIGGER_MODE}")
print(f"   Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Initialize Spark

# COMMAND ----------

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable

# Configure logging
spark.sparkContext.setLogLevel(LOG_LEVEL)
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("EventHubListener")

# Configure Spark for optimal streaming performance with Hive Metastore
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
#spark.conf.set("spark.sql.streaming.ui.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")

# Enable Hive Metastore support
#spark.conf.set("spark.sql.catalogImplementation", "hive")
#spark.conf.set("spark.sql.hive.metastore.jars", "builtin")

# Set checkpoint configuration for reliability
spark.conf.set("spark.sql.streaming.checkpointLocation.compression.codec", "lz4")
#spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


print("✅ Spark configured for structured streaming with Hive Metastore")
print(f"   Spark Version: {spark.version}")
print(f"   Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'unknown')}")
print(f"   Catalog Implementation: {spark.conf.get('spark.sql.catalogImplementation', 'unknown')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hive Metastore Management

# COMMAND ----------

class HiveMetastoreManager:
    """Manage Hive Metastore database and table operations"""
    
    def __init__(self, database_name: str, table_name: str):
        self.database_name = database_name
        self.table_name = table_name
        self.full_table_name = f"{database_name}.{table_name}"
    
    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            spark.sql(f"USE {self.database_name}")
            print(f"✅ Database '{self.database_name}' ensured and selected")
        except Exception as e:
            print(f"⚠️ Error creating database: {e}")
    
    def create_external_table(self, table_path: str, table_schema: StructType):
        """Create external Delta table in Hive Metastore"""
        try:
            # Check if table already exists
            table_exists = self._table_exists()
            
            if not table_exists:
                print(f"📝 Creating external table: {self.full_table_name}")
                
                # Build CREATE TABLE DDL for Delta format
                ddl_columns = []
                for field in table_schema.fields:
                    spark_type = field.dataType.simpleString()
                    nullable = "NULL" if field.nullable else "NOT NULL"
                    ddl_columns.append(f"`{field.name}` {spark_type} {nullable}")
                
                columns_ddl = ",\n  ".join(ddl_columns)
                
                create_table_sql = f"""
                CREATE TABLE {self.full_table_name} (
                  {columns_ddl}
                )
                USING DELTA
                LOCATION '{table_path}'
                PARTITIONED BY (ingestion_date, ingestion_hour)
                TBLPROPERTIES (
                  'delta.autoOptimize.optimizeWrite' = 'true',
                  'delta.autoOptimize.autoCompact' = 'true',
                  'delta.logRetentionDuration' = '30 days',
                  'delta.deletedFileRetentionDuration' = '7 days',
                  'description' = 'Event Hub raw data from {EVENTHUB_NAME}',
                  'data_source' = 'Azure Event Hub',
                  'ingestion_pattern' = 'Structured Streaming',
                  'data_format' = 'Raw JSON payload with technical metadata'
                )
                """
                
                spark.sql(create_table_sql)
                print(f"✅ Table '{self.full_table_name}' created successfully")
            else:
                print(f"✅ Table '{self.full_table_name}' already exists")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
    
    def _table_exists(self) -> bool:
        """Check if table exists in Hive Metastore"""
        try:
            spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            return True
        except:
            return False
    
    def show_table_info(self):
        """Display table information and metadata"""
        try:
            print(f"📊 Hive Metastore Table Information: {self.full_table_name}")
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
            
            # Show table properties
            print(f"\n🏷️ Table Properties:")
            properties = spark.sql(f"SHOW TBLPROPERTIES {self.full_table_name}").collect()
            for prop in properties[:10]:  # Show first 10 properties
                print(f"   {prop['key']}: {prop['value']}")
            
        except Exception as e:
            print(f"❌ Error retrieving table info: {e}")
    
    def show_databases(self):
        """Show available databases"""
        try:
            print("📚 Available Databases:")
            databases = spark.sql("SHOW DATABASES").collect()
            for db in databases:
                print(f"   - {db['databaseName']}")
        except Exception as e:
            print(f"⚠️ Error showing databases: {e}")
    
    def show_tables(self):
        """Show tables in current database"""
        try:
            print(f"📋 Tables in database '{self.database_name}':")
            tables = spark.sql(f"SHOW TABLES IN {self.database_name}").collect()
            for table in tables:
                print(f"   - {table['tableName']}")
        except Exception as e:
            print(f"⚠️ Error showing tables: {e}")

# Initialize Hive Metastore Manager
hive_manager = HiveMetastoreManager(DATABASE_NAME, TABLE_NAME)
print(f"✅ Hive Metastore manager initialized for: {FULL_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simplified Bronze Schema Definition

# COMMAND ----------

# Simplified Bronze schema: Technical fields + Raw payload
bronze_schema = StructType([
    # Technical/System Fields
    StructField("record_id", StringType(), False),  # Unique record identifier
    StructField("ingestion_timestamp", TimestampType(), False),  # When record was ingested
    StructField("ingestion_date", DateType(), False),  # Partitioning field
    StructField("ingestion_hour", IntegerType(), False),  # Partitioning field
    
    # Event Hub Technical Fields
    StructField("eventhub_partition", StringType(), True),
    StructField("eventhub_offset", StringType(), True),
    StructField("eventhub_sequence_number", LongType(), True),
    StructField("eventhub_enqueued_time", TimestampType(), True),
    StructField("eventhub_partition_key", StringType(), True),
    
    # Processing Context
    StructField("processing_cluster_id", StringType(), True),
    StructField("consumer_group", StringType(), True),
    StructField("eventhub_name", StringType(), True),
    
    # Raw Data Payload
    StructField("raw_payload", StringType(), True),  # Complete raw message as received
    StructField("payload_size_bytes", IntegerType(), True)  # Size of the payload
])

print("✅ Simplified Bronze schema defined:")
print("   - Technical fields for system metadata")
print("   - Event Hub fields for traceability") 
print("   - Raw payload preservation")
print("   - Append-only strategy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Event Hub Connection Configuration

# COMMAND ----------

class EventHubKafkaConnectionConfig:
    """Event Hub connection using Kafka API - eliminates Base64 decoding issues"""
    
    def __init__(self, secret_scope: str, eventhub_name: str, consumer_group: str):
        self.secret_scope = secret_scope
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        
        # Retrieve connection string from Databricks secrets
        try:
            self.connection_string = dbutils.secrets.get(scope=secret_scope, key="eventhub-connection-string")
            print("✅ Event Hub connection string retrieved from secrets")
            
            # Parse Event Hub connection string to extract Kafka parameters
            self._parse_connection_string()
            
            # Create Kafka configurations
            self._create_kafka_configurations()
            
        except Exception as e:
            raise ValueError(f"Failed to retrieve connection string from scope '{secret_scope}': {e}")
    
    def _parse_connection_string(self):
        """Parse Event Hub connection string to extract Kafka parameters"""
        import re
        
        # Extract endpoint
        endpoint_match = re.search(r'Endpoint=sb://([^/;]+)', self.connection_string)
        if not endpoint_match:
            raise ValueError("Could not extract endpoint from connection string")
        
        self.namespace = endpoint_match.group(1)
        self.kafka_brokers = f"{self.namespace}:9093"
        
        # Extract shared access key name
        key_name_match = re.search(r'SharedAccessKeyName=([^;]+)', self.connection_string)
        if not key_name_match:
            raise ValueError("Could not extract SharedAccessKeyName from connection string")
        
        self.key_name = key_name_match.group(1)
        
        # Extract shared access key
        key_match = re.search(r'SharedAccessKey=([^;]+)', self.connection_string)
        if not key_match:
            raise ValueError("Could not extract SharedAccessKey from connection string")
        
        self.key = key_match.group(1)
        
        print("✅ Connection string parsed for Kafka API")
    
    def _create_kafka_configurations(self):
        """Create Kafka configuration for Event Hub with fallback options"""
        
        # Build SASL configuration for Event Hub
        self.sasl_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{self.connection_string}";'
        
        # Primary Kafka configuration
        self.kafka_conf = {
            "kafka.bootstrap.servers": self.kafka_brokers,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": self.sasl_config,
            "kafka.request.timeout.ms": "60000",
            "kafka.session.timeout.ms": "30000",
            "subscribe": self.eventhub_name,
            "kafka.group.id": self.consumer_group,
            "startingOffsets": "latest",
            "maxOffsetsPerTrigger": str(MAX_EVENTS_PER_TRIGGER),
            "kafka.ssl.endpoint.identification.algorithm": ""
        }
        
        # Alternative configuration (fallback)
        self.kafka_conf_alt = {
            "kafka.bootstrap.servers": self.kafka_brokers,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{self.connection_string}";',
            "subscribe": self.eventhub_name,
            "kafka.group.id": self.consumer_group,
            "startingOffsets": "latest",
            "maxOffsetsPerTrigger": str(MAX_EVENTS_PER_TRIGGER)
        }
        
        print(f"✅ Kafka configuration prepared for Event Hub:")
        print(f"   Event Hub: {self.eventhub_name}")
        print(f"   Consumer Group: {self.consumer_group}")
        print(f"   Bootstrap Servers: {self.kafka_brokers}")
        print(f"   Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")
        print(f"   Protocol: SASL_SSL (eliminates Base64 issues)")

# Initialize Kafka-based connection configuration
eh_config = EventHubKafkaConnectionConfig(SECRET_SCOPE, EVENTHUB_NAME, CONSUMER_GROUP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ADLS Gen2 Configuration

# COMMAND ----------

class ADLSConfig:
    """ADLS Gen2 configuration and path management"""
    
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
    
    def get_bronze_path(self, base_path: str) -> str:
        """Get full bronze layer path"""
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

# Initialize ADLS configuration
adls_config = ADLSConfig(STORAGE_ACCOUNT, CONTAINER)
bronze_full_path = adls_config.get_bronze_path(BRONZE_PATH)
checkpoint_full_path = adls_config.get_checkpoint_path(CHECKPOINT_PATH)

print(f"✅ ADLS Gen2 configuration:")
print(f"   Bronze Path: {bronze_full_path}")
print(f"   Checkpoint Path: {checkpoint_full_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Initialize Hive Metastore Components

# COMMAND ----------

def setup_hive_metastore_components():
    """Setup database and create table in Hive Metastore"""
    
    print("🏗️ Setting up Hive Metastore components...")
    print("=" * 50)
    
    # Show current databases
    hive_manager.show_databases()
    
    # Create database if not exists
    hive_manager.create_database_if_not_exists()
    
    # Show tables in database
    hive_manager.show_tables()
    
    # Create external table
    success = hive_manager.create_external_table(bronze_full_path, bronze_schema)
    
    if success:
        print("✅ Hive Metastore setup completed successfully")
        print(f"📊 Table created: {FULL_TABLE_NAME}")
        
        # Show table information
        hive_manager.show_table_info()
        
        return True
    else:
        print("❌ Hive Metastore setup failed")
        return False

# Setup Hive Metastore components
hive_setup_success = setup_hive_metastore_components()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Stream Processing Functions

# COMMAND ----------

def process_eventhub_stream_simple(raw_stream: DataFrame) -> DataFrame:
    """Process raw Event Hub stream via Kafka API with simplified Bronze transformation"""
    
    # Get cluster context
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
    
    # Transform raw stream to Bronze format using Kafka field names
    bronze_stream = raw_stream.select(
        # Generate unique record ID using Kafka fields
        concat(
            col("partition"), 
            lit("-"), 
            col("offset"), 
            lit("-"), 
            unix_timestamp().cast("string")
        ).alias("record_id"),
        
        # System timestamps
        current_timestamp().alias("ingestion_timestamp"),
        current_date().alias("ingestion_date"),
        hour(current_timestamp()).alias("ingestion_hour"),
        
        # Event Hub technical fields (mapped from Kafka fields)
        col("partition").alias("eventhub_partition"),
        col("offset").alias("eventhub_offset"),
        
        # Kafka timestamp as Event Hub enqueued time equivalent
        when(col("timestamp").isNotNull(), 
             col("timestamp")
        ).otherwise(lit(None).cast("timestamp")).alias("eventhub_enqueued_time"),
        
        # Use Kafka timestamp milliseconds for sequence number equivalent
        when(col("timestamp").isNotNull(),
             unix_timestamp(col("timestamp")) * 1000
        ).otherwise(lit(None).cast("long")).alias("eventhub_sequence_number"),
        
        # Event Hub partition key (from Kafka key field)
        when(col("key").isNotNull(), 
             col("key").cast("string")
        ).otherwise(lit(None).cast("string")).alias("eventhub_partition_key"),
        
        # Processing context
        lit(cluster_id).alias("processing_cluster_id"),
        lit(CONSUMER_GROUP).alias("consumer_group"),
        lit(EVENTHUB_NAME).alias("eventhub_name"),
        
        # Raw payload preservation - Kafka API eliminates Base64 encoding issues
        # Kafka connector returns value as binary, decode as UTF-8 string directly
        when(col("value").isNotNull(), 
             col("value").cast("string")
        ).otherwise(lit("")).alias("raw_payload"),
        
        # Payload size using Kafka value field
        when(col("value").isNotNull(),
             length(col("value"))
        ).otherwise(lit(0)).alias("payload_size_bytes")
    )
    
    return bronze_stream

def get_trigger_config(trigger_mode: str):
    """Get trigger configuration based on mode"""
    if trigger_mode == "continuous":
        return {"continuous": "1 second"}
    elif "second" in trigger_mode:
        interval = trigger_mode.split()[0]
        return {"processingTime": f"{interval} seconds"}
    elif "minute" in trigger_mode:
        interval = trigger_mode.split()[0]
        return {"processingTime": f"{interval} minutes"}
    else:
        return {"processingTime": "5 seconds"}

print("✅ Simplified stream processing functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Stream Monitoring

# COMMAND ----------

class StreamMonitor:
    """Monitor streaming job health and performance"""
    
    def __init__(self, query_name: str, table_name: str):
        self.query_name = query_name
        self.table_name = table_name
        
    def display_metrics_dashboard(self, query: StreamingQuery):
        """Display real-time metrics dashboard"""
        try:
            progress = query.lastProgress
            if progress:
                batch_id = progress.get('batchId', 0)
                input_rows = progress.get('inputRowsPerSecond', 0)
                processed_rows = progress.get('inputRowsPerSecond', 0)
                batch_duration = progress.get('durationMs', {}).get('triggerExecution', 0)
                
                dashboard_html = f"""
                <div style="border: 2px solid #2196F3; border-radius: 10px; padding: 15px; background: #f8f9fa;">
                    <h3 style="color: #2196F3;">📡 Event Hub Streaming - Hive Metastore Integration</h3>
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 15px 0;">
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
                    </div>
                    <div style="margin-top: 15px; background: #e3f2fd; padding: 10px; border-radius: 5px;">
                        <strong>Hive Table:</strong> {self.table_name} | 
                        <strong>Strategy:</strong> Append-Only | 
                        <strong>Status:</strong> {'🟢 Active' if query.isActive else '🔴 Stopped'}
                    </div>
                </div>
                """
                
                displayHTML(dashboard_html)
        except Exception as e:
            print(f"⚠️ Dashboard update error: {e}")
    
    def check_stream_health(self, query: StreamingQuery) -> Dict[str, Any]:
        """Check streaming job health status"""
        health_status = {
            "is_active": query.isActive,
            "query_id": query.id,
            "run_id": query.runId,
            "status": "healthy" if query.isActive else "stopped",
            "table_name": self.table_name
        }
        
        try:
            if query.exception():
                health_status["status"] = "error"
                health_status["exception"] = str(query.exception())
        except:
            pass
        
        return health_status

# Initialize monitoring
monitor = StreamMonitor("weather-eventhub-hive-listener", FULL_TABLE_NAME)
print("✅ Stream monitoring initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Main Streaming Job with Hive Metastore Integration

# COMMAND ----------

def start_eventhub_streaming_with_hive():
    """Start the Event Hub streaming job with Hive Metastore table integration"""
    
    if not hive_setup_success:
        print("❌ Cannot start streaming - Hive Metastore setup failed")
        return None
    
    print("🚀 Starting Event Hub Streaming Job - Hive Metastore Integration")
    print("=" * 60)
    print(f"📡 Source: Event Hub '{EVENTHUB_NAME}'")
    print(f"🏢 Consumer Group: {CONSUMER_GROUP}")
    print(f"💾 Destination: {bronze_full_path}")
    print(f"🗄️ Hive Table: {FULL_TABLE_NAME}")
    print(f"🔄 Checkpoint: {checkpoint_full_path}")
    print(f"⚡ Trigger: {TRIGGER_MODE}")
    print(f"📦 Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")
    print(f"📋 Strategy: Append-Only (No Validation)")
    print("=" * 60)
    
    try:
        # Create Event Hub input stream using Kafka API (eliminates Base64 issues)
        print("🔄 Attempting Kafka API connection to Event Hub...")
        
        try:
            # Try primary Kafka configuration
            raw_stream = spark \
                .readStream \
                .format("kafka") \
                .options(**eh_config.kafka_conf) \
                .load()
            
            print("✅ Event Hub stream source created via Kafka API (primary config)")
            
        except Exception as e:
            print(f"⚠️ Primary Kafka config failed: {str(e)[:100]}...")
            print("🔄 Trying alternative Kafka configuration...")
            
            # Try alternative Kafka configuration
            raw_stream = spark \
                .readStream \
                .format("kafka") \
                .options(**eh_config.kafka_conf_alt) \
                .load()
            
            print("✅ Event Hub stream source created via Kafka API (alternative config)")
        
        # Process the stream via Kafka API (eliminates Base64 issues)
        processed_stream = process_eventhub_stream_simple(raw_stream)
        
        print("✅ Stream processing pipeline configured via Kafka API (eliminates Base64 issues)")
        
        # Configure trigger
        trigger_config = get_trigger_config(TRIGGER_MODE)
        print(f"✅ Trigger configured: {trigger_config}")
        
        # Write to Hive Metastore table with checkpointing - APPEND ONLY
        query = processed_stream \
            .writeStream \
            .outputMode("append") \
            .format("delta") \
            .option("checkpointLocation", checkpoint_full_path) \
            .trigger(**trigger_config) \
            .toTable(FULL_TABLE_NAME)
        
        print("✅ Streaming query started successfully")
        print(f"📊 Query ID: {query.id}")
        print(f"🔄 Run ID: {query.runId}")
        print(f"📝 Mode: APPEND-ONLY")
        print(f"🗄️ Writing to Hive Metastore table: {FULL_TABLE_NAME}")
        
        return query
        
    except Exception as e:
        print(f"❌ Failed to start streaming job: {e}")
        raise

# Start the streaming job
streaming_query = start_eventhub_streaming_with_hive()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Monitor Streaming Job
# MAGIC 
# MAGIC **⚠️ Important:** The streaming job is now running and writing to the Hive Metastore table.

# COMMAND ----------

import time

if streaming_query:
    # Monitor the streaming job
    print("📊 Monitoring Event Hub Streaming Job - Hive Metastore Table")
    print(f"🗄️ Table: {FULL_TABLE_NAME}")
    print("Strategy: Append-Only (No Validation)")
    print("Use Ctrl+C or the stop command in the next cell to terminate")
    print("-" * 60)
    
    try:
        # Monitor loop
        for i in range(10):  # Monitor for 10 iterations, then continue in background
            if streaming_query.isActive:
                # Display metrics
                monitor.display_metrics_dashboard(streaming_query)
                
                # Log health status
                health = monitor.check_stream_health(streaming_query)
                print(f"Status: {health['status'].upper()} | Active: {health['is_active']}")
                
                # Wait before next check
                time.sleep(30)  # Check every 30 seconds
            else:
                print("❌ Streaming query is not active")
                break
        
        if streaming_query.isActive:
            print("✅ Streaming job is running in background")
            print(f"📝 Mode: APPEND-ONLY - All records preserved in {FULL_TABLE_NAME}")
            print("Use the management cells below to control the stream")
        
    except KeyboardInterrupt:
        print("⏹️ Monitoring interrupted by user")
    except Exception as e:
        print(f"⚠️ Monitoring error: {e}")
else:
    print("❌ Streaming query not started due to setup issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Hive Table Management Commands

# COMMAND ----------

def query_hive_table():
    """Query the Hive Metastore table directly"""
    try:
        print(f"📊 Querying Hive Metastore Table: {FULL_TABLE_NAME}")
        print("=" * 60)
        
        # Count total records
        total_count = spark.table(FULL_TABLE_NAME).count()
        print(f"📈 Total Records: {total_count:,}")
        
        if total_count > 0:
            # Show recent records
            print(f"\n🕒 Latest Records:")
            spark.table(FULL_TABLE_NAME) \
                .orderBy(col("ingestion_timestamp").desc()) \
                .select("record_id", "ingestion_timestamp", "eventhub_partition", 
                       "payload_size_bytes", "eventhub_name") \
                .show(5, truncate=False)
            
            # Partition statistics
            print(f"\n📁 Records by Date:")
            spark.table(FULL_TABLE_NAME) \
                .groupBy("ingestion_date") \
                .count() \
                .orderBy("ingestion_date") \
                .show(10)
            
            # Payload size statistics
            payload_stats = spark.table(FULL_TABLE_NAME) \
                .agg(avg("payload_size_bytes").alias("avg_size"),
                     min("payload_size_bytes").alias("min_size"),
                     max("payload_size_bytes").alias("max_size")) \
                .collect()[0]
            
            print(f"\n📊 Payload Statistics:")
            print(f"   Average Size: {payload_stats['avg_size']:.1f} bytes")
            print(f"   Size Range: {payload_stats['min_size']} - {payload_stats['max_size']} bytes")
        
    except Exception as e:
        print(f"❌ Error querying table: {e}")

def show_hive_table_metadata():
    """Display comprehensive Hive table metadata"""
    try:
        print(f"📋 Hive Table Metadata: {FULL_TABLE_NAME}")
        print("=" * 60)
        
        # Table description
        print("📝 Table Description:")
        spark.sql(f"DESCRIBE EXTENDED {FULL_TABLE_NAME}").show(50, truncate=False)
        
        # Table properties
        print(f"\n🏷️ Table Properties:")
        properties = spark.sql(f"SHOW TBLPROPERTIES {FULL_TABLE_NAME}").collect()
        for prop in properties:
            print(f"   {prop['key']}: {prop['value']}")
        
        # Show partitions if available
        try:
            print(f"\n📂 Table Partitions:")
            spark.sql(f"SHOW PARTITIONS {FULL_TABLE_NAME}").show(20, truncate=False)
        except:
            print("   No partitions found (empty table)")
        
    except Exception as e:
        print(f"❌ Error showing metadata: {e}")

def optimize_hive_table():
    """Optimize the Hive Metastore table"""
    try:
        print(f"🔧 Optimizing Hive Metastore Table: {FULL_TABLE_NAME}")
        
        # OPTIMIZE with Z-ORDER
        spark.sql(f"""
            OPTIMIZE {FULL_TABLE_NAME}
            ZORDER BY (ingestion_date, eventhub_partition, ingestion_timestamp)
        """)
        
        print("✅ Table optimization completed")
        
        # VACUUM old files
        print("🧹 Cleaning up old files...")
        spark.sql(f"VACUUM {FULL_TABLE_NAME} RETAIN 168 HOURS")
        
        print("✅ Table maintenance completed")
        
    except Exception as e:
        print(f"❌ Error optimizing table: {e}")

def show_hive_databases_and_tables():
    """Show Hive databases and tables"""
    print("📚 Hive Metastore Overview:")
    print("=" * 40)
    
    # Show databases
    hive_manager.show_databases()
    print()
    
    # Show tables in current database
    hive_manager.show_tables()
    print()
    
    # Show current database
    current_db = spark.sql("SELECT current_database()").collect()[0][0]
    print(f"🎯 Current Database: {current_db}")

# Run table query
query_hive_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Stream Management Commands

# COMMAND ----------

# Check current status
def check_streaming_status():
    """Check current streaming job status"""
    if streaming_query and (streaming_query in locals() or streaming_query in globals()):
        health = monitor.check_stream_health(streaming_query)
        print(f"📊 Streaming Job Status:")
        print(f"   Query ID: {health['query_id']}")
        print(f"   Run ID: {health['run_id']}")
        print(f"   Status: {health['status'].upper()}")
        print(f"   Active: {health['is_active']}")
        print(f"   Target Hive Table: {health['table_name']}")
        print(f"   Mode: APPEND-ONLY")
        
        if streaming_query.isActive:
            progress = streaming_query.lastProgress
            if progress:
                print(f"   Last Batch: {progress.get('batchId', 'N/A')}")
                print(f"   Processed Rows/sec: {progress.get('inputRowsPerSecond', 0):.1f}")
        
        return health
    else:
        print("❌ No streaming query found")
        return None

def stop_streaming_job():
    """Stop the streaming job gracefully"""
    try:
        if streaming_query and streaming_query.isActive:
            print("⏹️ Stopping streaming job...")
            streaming_query.stop()
            
            # Wait for graceful shutdown
            timeout = 30  # 30 seconds timeout
            elapsed = 0
            while streaming_query.isActive and elapsed < timeout:
                time.sleep(1)
                elapsed += 1
            
            if not streaming_query.isActive:
                print("✅ Streaming job stopped successfully")
                print(f"📊 Final data available in Hive table: {FULL_TABLE_NAME}")
            else:
                print("⚠️ Streaming job stop timeout - may still be shutting down")
        else:
            print("ℹ️ Streaming job is already stopped")
            
    except Exception as e:
        print(f"❌ Error stopping streaming job: {e}")

# Display current status
if streaming_query:
    status = check_streaming_status()
else:
    print("❌ Streaming query not available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. SQL Analytics Examples

# COMMAND ----------

def show_sql_examples():
    """Show SQL query examples for the Hive table"""
    
    print("📊 SQL Analytics Examples for Hive Metastore Table")
    print("=" * 60)
    
    examples = [
        {
            "title": "Basic Data Query",
            "sql": f"""
-- Query recent records
SELECT record_id, ingestion_timestamp, eventhub_partition, payload_size_bytes
FROM {FULL_TABLE_NAME}
ORDER BY ingestion_timestamp DESC
LIMIT 10;
"""
        },
        {
            "title": "Daily Ingestion Summary",
            "sql": f"""
-- Daily ingestion statistics
SELECT 
    ingestion_date,
    COUNT(*) as record_count,
    AVG(payload_size_bytes) as avg_payload_size,
    MIN(ingestion_timestamp) as first_record,
    MAX(ingestion_timestamp) as last_record
FROM {FULL_TABLE_NAME}
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;
"""
        },
        {
            "title": "Event Hub Partition Analysis",
            "sql": f"""
-- Event Hub partition distribution
SELECT 
    eventhub_partition,
    COUNT(*) as message_count,
    MIN(eventhub_enqueued_time) as earliest_message,
    MAX(eventhub_enqueued_time) as latest_message
FROM {FULL_TABLE_NAME}
GROUP BY eventhub_partition
ORDER BY eventhub_partition;
"""
        },
        {
            "title": "Payload Content Analysis",
            "sql": f"""
-- Analyze payload patterns
SELECT 
    payload_size_bytes,
    COUNT(*) as frequency,
    SUBSTRING(raw_payload, 1, 50) as sample_payload
FROM {FULL_TABLE_NAME}
GROUP BY payload_size_bytes, SUBSTRING(raw_payload, 1, 50)
ORDER BY frequency DESC
LIMIT 20;
"""
        },
        {
            "title": "Hourly Throughput Analysis",
            "sql": f"""
-- Hourly ingestion throughput
SELECT 
    ingestion_date,
    ingestion_hour,
    COUNT(*) as records_per_hour,
    SUM(payload_size_bytes) as total_bytes_per_hour
FROM {FULL_TABLE_NAME}
GROUP BY ingestion_date, ingestion_hour
ORDER BY ingestion_date DESC, ingestion_hour DESC;
"""
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"\n{i}. {example['title']}:")
        print(f"{example['sql']}")
    
    print(f"\n💡 Usage Tips:")
    print(f"   - Use these queries in Databricks SQL Analytics")
    print(f"   - Create dashboards with visualization")
    print(f"   - Set up alerts on data quality metrics")
    print(f"   - Table location: {bronze_full_path}")

# Show SQL examples
show_sql_examples()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Final Summary and Next Steps

# COMMAND ----------

def display_hive_integration_summary():
    """Display comprehensive summary including Hive Metastore integration"""
    
    summary_html = f"""
    <div style="border: 2px solid #4CAF50; border-radius: 15px; padding: 20px; background: #f8f9fa;">
        <h2 style="color: #4CAF50; text-align: center;">📡 Event Hub Streaming with Hive Metastore</h2>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin: 20px 0;">
            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #2196F3;">
                <h3 style="color: #2196F3; margin-top: 0;">🔧 Configuration</h3>
                <ul style="margin: 0;">
                    <li><strong>Event Hub:</strong> {EVENTHUB_NAME}</li>
                    <li><strong>Consumer Group:</strong> {CONSUMER_GROUP}</li>
                    <li><strong>Trigger:</strong> {TRIGGER_MODE}</li>
                    <li><strong>Max Events/Trigger:</strong> {MAX_EVENTS_PER_TRIGGER:,}</li>
                </ul>
            </div>
            
            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #FF9800;">
                <h3 style="color: #FF9800; margin-top: 0;">🗄️ Hive Metastore</h3>
                <ul style="margin: 0;">
                    <li><strong>Database:</strong> {DATABASE_NAME}</li>
                    <li><strong>Table:</strong> {TABLE_NAME}</li>
                    <li><strong>Full Name:</strong> {FULL_TABLE_NAME}</li>
                    <li><strong>Format:</strong> Delta Lake External</li>
                </ul>
            </div>
        </div>
        
        <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #9C27B0; margin: 20px 0;">
            <h3 style="color: #9C27B0; margin-top: 0;">✨ Features Implemented</h3>
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
                <div>✅ Hive Metastore Integration</div>
                <div>✅ External Delta Table</div>
                <div>✅ Raw Data Preservation</div>
                <div>✅ Append-Only Strategy</div>
                <div>✅ Checkpoint Management</div>
                <div>✅ Real-time Monitoring</div>
                <div>✅ Partitioned Storage</div>
                <div>✅ SQL Analytics Ready</div>
                <div>✅ Table Optimization</div>
            </div>
        </div>
        
        <div style="background: #e8f5e8; padding: 15px; border-radius: 10px; margin: 20px 0;">
            <h3 style="color: #2e7d2e; margin-top: 0;">🎯 Next Steps</h3>
            <ol style="margin: 0;">
                <li><strong>Query:</strong> Use Databricks SQL or notebooks to query {FULL_TABLE_NAME}</li>
                <li><strong>Visualize:</strong> Create dashboards in Databricks SQL Analytics</li>
                <li><strong>Optimize:</strong> Run table optimization for better performance</li>
                <li><strong>Transform:</strong> Build Silver/Gold layers with business logic</li>
                <li><strong>Monitor:</strong> Set up alerts and data quality checks</li>
            </ol>
        </div>
    </div>
    """
    
    displayHTML(summary_html)
    
    print("📊 Complete Hive Integration Summary:")
    print("=" * 60)
    print(f"✅ Event Hub: {EVENTHUB_NAME} → Streaming → Hive Metastore")
    print(f"✅ Hive Table: {FULL_TABLE_NAME}")
    print(f"✅ Storage: Delta Lake format in ADLS Gen2")
    print(f"✅ Strategy: Append-Only (No Validation)")
    print(f"✅ Governance: Hive Metastore catalog integration")
    print(f"✅ Analytics: SQL-ready for reporting and analysis")
    print("=" * 60)
    
    # Table accessibility
    print(f"\n🔍 Table Access:")
    print(f"   SQL: SELECT * FROM {FULL_TABLE_NAME}")
    print(f"   Python: spark.table('{FULL_TABLE_NAME}')")
    print(f"   Location: {bronze_full_path}")
    print(f"   Database: {DATABASE_NAME}")
    
    if streaming_query and streaming_query.isActive:
        print("\n🚀 Status: Streaming job is ACTIVE and ingesting to Hive Metastore")
    else:
        print("\n⏹️ Status: Streaming job is STOPPED")
    
    print("\n💡 Available Commands:")
    print("   - query_hive_table(): Query table directly")
    print("   - show_hive_table_metadata(): Display table properties")
    print("   - optimize_hive_table(): Optimize table performance")
    print("   - show_hive_databases_and_tables(): Browse Hive catalog")
    print("   - stop_streaming_job(): Stop the streaming job")

# Display complete summary
display_hive_integration_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Hive Metastore Integration Documentation
# MAGIC 
# MAGIC ### Complete Architecture
# MAGIC ```
# MAGIC Azure Event Hub (via Kafka API) → Spark Structured Streaming → Delta Lake → ADLS Gen2 → Hive Metastore
# MAGIC ```
# MAGIC 
# MAGIC ### Hive Metastore Components
# MAGIC - **Database**: `bronze` - Logical database grouping
# MAGIC - **Table**: `weather_events_raw` - Raw event data table
# MAGIC - **Full Name**: `bronze.weather_events_raw`
# MAGIC - **Type**: External Delta table with ADLS Gen2 storage
# MAGIC 
# MAGIC ### Data Governance Features
# MAGIC - ✅ **Hive Metastore Integration**: Native Databricks catalog support
# MAGIC - ✅ **External Table**: Delta Lake files in ADLS Gen2 with Hive registration
# MAGIC - ✅ **Partitioning**: By ingestion_date and ingestion_hour
# MAGIC - ✅ **SQL Analytics**: Direct table querying capabilities
# MAGIC - ✅ **Table Properties**: Metadata and optimization settings
# MAGIC 
# MAGIC ### Usage Examples
# MAGIC ```sql
# MAGIC -- Query the table directly
# MAGIC SELECT * FROM bronze.weather_events_raw 
# MAGIC WHERE ingestion_date = current_date()
# MAGIC LIMIT 100;
# MAGIC 
# MAGIC -- Analyze throughput patterns
# MAGIC SELECT eventhub_partition, COUNT(*), AVG(payload_size_bytes)
# MAGIC FROM bronze.weather_events_raw
# MAGIC GROUP BY eventhub_partition;
# MAGIC 
# MAGIC -- Check recent data
# MAGIC SELECT COUNT(*) as recent_records
# MAGIC FROM bronze.weather_events_raw
# MAGIC WHERE ingestion_timestamp >= current_timestamp() - INTERVAL 1 HOUR;
# MAGIC ```
# MAGIC 
# MAGIC ### Benefits
# MAGIC - **Discoverability**: Table available in Databricks Hive catalog
# MAGIC - **SQL Compatibility**: Standard Hive/Spark SQL access
# MAGIC - **Performance**: Delta Lake optimizations with Hive integration
# MAGIC - **Analytics**: Direct querying from Databricks SQL, notebooks, and BI tools
# MAGIC - **Metadata**: Rich table properties and partitioning information
# MAGIC 
# MAGIC **🎯 Your Event Hub data is now fully integrated into the Hive Metastore with complete SQL analytics capabilities.**