# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item EventHub Listener
# MAGIC
# MAGIC EventHub to Bronze layer streaming pipeline for purchase order items.
# MAGIC
# MAGIC ## Architecture:
# MAGIC ```
# MAGIC Azure EventHub (purchase-order-items) → EventHub Listener → Bronze Layer (ADLS Gen2 + Hive Metastore)
# MAGIC ```
# MAGIC
# MAGIC ## Features:
# MAGIC - Structured streaming from EventHub
# MAGIC - Bronze layer with technical metadata
# MAGIC - ADLS Gen2 integration with partitioning
# MAGIC - Hive Metastore table management
# MAGIC - Data quality monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import Required Classes and Libraries

# COMMAND ----------

# Import existing classes from organized structure
import sys
sys.path.append("../class")

from purchase_order_item_listener import PurchaseOrderItemListener, create_listener_from_widgets
from bronze_layer_handler import BronzeLayerHandler
from hive_metastore_manager import HiveMetastoreManager, create_hive_manager_from_widgets

# Standard libraries
import json
import logging
from datetime import datetime
from pyspark.sql.functions import col, count, avg, sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Widget Configuration (Exact Pattern)

# COMMAND ----------

# EventHub configuration (EXACT same pattern as reference)
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("eventhub_name", "purchase-order-items", "Event Hub Name")
dbutils.widgets.text("consumer_group", "$Default", "Consumer Group")
dbutils.widgets.text("bronze_path", "/mnt/bronze/purchase_orders", "Bronze Layer Path (Local DBFS)")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/purchase-order-listener", "Checkpoint Path (Local DBFS)")
dbutils.widgets.text("storage_account", "idisitcusadls2", "Storage Account Name")
dbutils.widgets.text("container", "purchase-order-test", "ADLS Container Name")

# Hive Metastore table configuration (EXACT same pattern as reference)
dbutils.widgets.text("database_name", "bronze", "Database Name")
dbutils.widgets.text("table_name", "purchase_order_items_raw", "Table Name")

# Streaming configuration (EXACT same pattern as reference)
dbutils.widgets.dropdown("trigger_mode", "5 seconds", ["1 second", "5 seconds", "10 seconds", "1 minute", "continuous"], "Trigger Interval")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")
dbutils.widgets.text("max_events_per_trigger", "10000", "Max Events Per Trigger")

# Additional monitoring configuration
dbutils.widgets.dropdown("auto_create_table", "true", ["true", "false"], "Auto Create Hive Table")
dbutils.widgets.dropdown("enable_monitoring", "true", ["true", "false"], "Enable Monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get Widget Values and Configuration

# COMMAND ----------

# Get widget values
SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CONSUMER_GROUP = dbutils.widgets.get("consumer_group")
BRONZE_PATH = dbutils.widgets.get("bronze_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
CONTAINER = dbutils.widgets.get("container")
DATABASE_NAME = dbutils.widgets.get("database_name")
TABLE_NAME = dbutils.widgets.get("table_name")
TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
LOG_LEVEL = dbutils.widgets.get("log_level")
MAX_EVENTS = int(dbutils.widgets.get("max_events_per_trigger"))
AUTO_CREATE_TABLE = dbutils.widgets.get("auto_create_table").lower() == "true"
ENABLE_MONITORING = dbutils.widgets.get("enable_monitoring").lower() == "true"

# Display configuration
print("🎯 PURCHASE ORDER EVENTHUB LISTENER CONFIGURATION")
print("=" * 80)
print(f"EventHub Name:       {EVENTHUB_NAME}")
print(f"Consumer Group:      {CONSUMER_GROUP}")
print(f"Bronze Path:         {BRONZE_PATH}")
print(f"Checkpoint Path:     {CHECKPOINT_PATH}")
print(f"Storage Account:     {STORAGE_ACCOUNT}")
print(f"Container:           {CONTAINER}")
print(f"Database:            {DATABASE_NAME}")
print(f"Table:               {TABLE_NAME}")
print(f"Trigger Mode:        {TRIGGER_MODE}")
print(f"Max Events/Trigger:  {MAX_EVENTS:,}")
print(f"Auto Create Table:   {AUTO_CREATE_TABLE}")
print(f"Enable Monitoring:   {ENABLE_MONITORING}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Setup Components and Dependencies

# COMMAND ----------

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("PurchaseOrderListener")

# Get connection string from secret scope
try:
    connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{EVENTHUB_NAME}-connection-string")
    logger.info("✅ Retrieved EventHub connection string from secret scope")
except Exception as e:
    logger.error(f"❌ Failed to retrieve connection string: {e}")
    raise

# Build configuration objects
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

print("✅ Configuration loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Initialize Handler Classes

# COMMAND ----------

# Initialize Bronze Layer Handler
bronze_handler = BronzeLayerHandler(
    spark=spark,
    storage_account=STORAGE_ACCOUNT,
    container=CONTAINER,
    bronze_path=BRONZE_PATH,
    log_level=LOG_LEVEL
)

# Initialize Hive Metastore Manager
hive_manager = HiveMetastoreManager(
    spark=spark,
    database_name=DATABASE_NAME,
    table_name=TABLE_NAME,
    bronze_path=bronze_handler.get_full_path(),
    log_level=LOG_LEVEL
)

# Initialize Purchase Order Listener
listener = PurchaseOrderItemListener(
    spark=spark,
    eventhub_config=eventhub_config,
    bronze_config=bronze_config,
    streaming_config=streaming_config,
    log_level=LOG_LEVEL
)

print("✅ All handler classes initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Setup Bronze Layer Infrastructure

# COMMAND ----------

print("🏗️  Setting up Bronze Layer Infrastructure...")

# Create directory structure
try:
    bronze_handler.create_directory_structure()
    print("✅ Bronze layer directory structure created")
except Exception as e:
    print(f"⚠️  Directory structure: {e}")

# Create Hive database and table if requested
if AUTO_CREATE_TABLE:
    try:
        # Create database
        database_created = hive_manager.create_database()
        if database_created:
            print(f"✅ Database '{DATABASE_NAME}' ready")

        # Create table
        table_created = hive_manager.create_table()
        if table_created:
            print(f"✅ Table '{DATABASE_NAME}.{TABLE_NAME}' ready")

        # Update table statistics
        hive_manager.update_table_statistics()
        print("✅ Table statistics updated")

    except Exception as e:
        print(f"⚠️  Table creation: {e}")
        if not hive_manager.table_exists():
            print("⚠️  Table does not exist - will be created automatically during streaming")

print("🎯 Bronze layer infrastructure setup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Bronze Layer Schema Validation

# COMMAND ----------

print("📋 BRONZE LAYER SCHEMA DEFINITION")
print("-" * 60)

# Display the Bronze schema
schema_fields = listener.bronze_schema.fields
print("Bronze Layer Fields:")
for i, field in enumerate(schema_fields, 1):
    nullable = "NULL" if field.nullable else "NOT NULL"
    print(f"  {i:2d}. {field.name:<25} {str(field.dataType):<15} {nullable}")

print(f"\nTotal Fields: {len(schema_fields)}")
print("Partition Strategy: ingestion_date, ingestion_hour")
print("Storage Format: Parquet with Snappy compression")
print("Path Structure: /ingestion_date=YYYY-MM-DD/ingestion_hour=HH/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Start Streaming Process

# COMMAND ----------

print("🚀 Starting Purchase Order Streaming...")
print(f"⏱️  Trigger Mode: {TRIGGER_MODE}")
print(f"📊 Max Events per Trigger: {MAX_EVENTS:,}")
print("-" * 60)

try:
    # Start the streaming query
    stream_query = listener.start_streaming(
        output_mode="append",
        trigger_interval=TRIGGER_MODE
    )

    if stream_query:
        print("✅ Streaming query started successfully!")
        print(f"📊 Query ID: {stream_query.id}")
        print(f"🏃 Run ID: {stream_query.runId}")
        print(f"📍 Checkpoint: {CHECKPOINT_PATH}")
        print(f"💾 Output Path: {bronze_handler.get_full_path()}")

        # Display initial status
        initial_status = listener.get_stream_status()
        print(f"🔄 Stream Active: {initial_status['is_active']}")

    else:
        raise Exception("Failed to start streaming query")

except Exception as e:
    print(f"❌ Failed to start streaming: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Monitoring and Status Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 **Streaming Status** (Run this cell repeatedly to monitor)

# COMMAND ----------

# Get current stream status
status = listener.get_stream_status()

print("📊 REAL-TIME STREAMING STATUS")
print("=" * 50)
print(f"⏰ Current Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"🔄 Stream Active: {'✅ YES' if status['is_active'] else '❌ NO'}")

if status['is_active']:
    stats = status['statistics']
    print(f"📦 Total Records: {stats['total_records_processed']:,}")

    if stats['stream_start_time']:
        start_time = datetime.fromisoformat(stats['stream_start_time'].replace('Z', '+00:00'))
        elapsed = (datetime.utcnow() - start_time.replace(tzinfo=None)).total_seconds()
        rate = stats['total_records_processed'] / elapsed if elapsed > 0 else 0
        print(f"⏱️  Runtime: {elapsed/60:.1f} minutes")
        print(f"📈 Processing Rate: {rate:.2f} records/second")

    if stats['last_batch_time']:
        last_batch = datetime.fromisoformat(stats['last_batch_time'].replace('Z', '+00:00'))
        time_since = (datetime.utcnow() - last_batch.replace(tzinfo=None)).total_seconds()
        print(f"🕐 Last Batch: {time_since:.0f} seconds ago")

    # Recent batches info
    if 'recent_batches' in status and status['recent_batches']:
        print(f"\n📋 Recent Batches:")
        for batch in status['recent_batches'][-3:]:  # Last 3 batches
            print(f"   Batch {batch['id']}: {batch.get('processed_rows', 'N/A')} rows/sec")

    # Error status
    if stats['errors']:
        print(f"\n⚠️  Errors: {len(stats['errors'])}")
        for error in stats['errors'][-2:]:  # Last 2 errors
            print(f"   - {error}")

else:
    print("❌ Stream is not active")

print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 **Bronze Layer Statistics** (Run periodically)

# COMMAND ----------

# Check Bronze layer data
try:
    # Get storage statistics
    storage_stats = bronze_handler.get_storage_statistics()

    print("💾 BRONZE LAYER STORAGE STATISTICS")
    print("-" * 50)
    print(f"📍 Base Path: {storage_stats.get('base_path', 'N/A')}")
    print(f"📊 Record Count: {storage_stats.get('record_count', 0):,}")
    print(f"📁 File Count: {storage_stats.get('file_count', 0):,}")
    print(f"💾 Total Size: {storage_stats.get('total_size_mb', 0):.2f} MB")
    print(f"📂 Partitions: {storage_stats.get('partition_count', 0)}")

    if 'partitions' in storage_stats and storage_stats['partitions']:
        print(f"\n📋 Recent Partitions:")
        for partition in storage_stats['partitions'][:5]:
            print(f"   - {partition}")

except Exception as e:
    print(f"⚠️  Could not retrieve storage statistics: {e}")

# Check Hive table statistics
if hive_manager.table_exists():
    try:
        table_stats = hive_manager.get_table_statistics()

        print(f"\n🗄️  HIVE TABLE STATISTICS")
        print("-" * 50)
        print(f"📊 Table Records: {table_stats.get('record_count', 0):,}")
        print(f"📂 Table Partitions: {table_stats.get('partition_count', 0)}")

        # Quality summary
        if 'quality_summary' in table_stats:
            print(f"\n🎯 Data Quality Summary:")
            for validity, stats in table_stats['quality_summary'].items():
                print(f"   {validity}: {stats['count']:,} records (avg score: {stats['avg_quality_score']:.3f})")

        # Recent partitions
        if 'recent_partitions' in table_stats and table_stats['recent_partitions']:
            print(f"\n📅 Recent Data:")
            for partition in table_stats['recent_partitions'][:3]:
                print(f"   {partition['date']} Hour {partition['hour']:02d}: {partition['records']:,} records")

    except Exception as e:
        print(f"⚠️  Could not retrieve table statistics: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Data Quality Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 **Quality Analysis** (Run to check data quality)

# COMMAND ----------

if hive_manager.table_exists():
    try:
        print("🎯 DATA QUALITY ANALYSIS")
        print("-" * 40)

        # Quality distribution
        quality_df = spark.sql(f"""
            SELECT
                is_valid,
                COUNT(*) as record_count,
                AVG(data_quality_score) as avg_score,
                MIN(data_quality_score) as min_score,
                MAX(data_quality_score) as max_score
            FROM {DATABASE_NAME}.{TABLE_NAME}
            GROUP BY is_valid
            ORDER BY is_valid DESC
        """)

        print("📊 Quality Distribution:")
        quality_results = quality_df.collect()
        total_records = sum(row.record_count for row in quality_results)

        for row in quality_results:
            percentage = (row.record_count / total_records * 100) if total_records > 0 else 0
            print(f"   Valid={row.is_valid}: {row.record_count:,} records ({percentage:.1f}%)")
            print(f"      Avg Score: {row.avg_score:.3f}, Range: {row.min_score:.3f}-{row.max_score:.3f}")

        # Order status distribution
        status_df = spark.sql(f"""
            SELECT
                order_status,
                payment_status,
                COUNT(*) as count
            FROM {DATABASE_NAME}.{TABLE_NAME}
            WHERE order_status IS NOT NULL
            GROUP BY order_status, payment_status
            ORDER BY count DESC
            LIMIT 10
        """)

        print(f"\n📋 Order Status Distribution (Top 10):")
        for row in status_df.collect():
            print(f"   {row.order_status} / {row.payment_status}: {row.count:,}")

        # Financial summary
        financial_df = spark.sql(f"""
            SELECT
                COUNT(*) as total_orders,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                MIN(total_amount) as min_order,
                MAX(total_amount) as max_order
            FROM {DATABASE_NAME}.{TABLE_NAME}
            WHERE total_amount IS NOT NULL
        """)

        print(f"\n💰 Financial Summary:")
        for row in financial_df.collect():
            print(f"   Total Orders: {row.total_orders:,}")
            print(f"   Total Revenue: ${row.total_revenue:,.2f}")
            print(f"   Avg Order Value: ${row.avg_order_value:.2f}")
            print(f"   Order Range: ${row.min_order:.2f} - ${row.max_order:,.2f}")

    except Exception as e:
        print(f"⚠️  Quality analysis failed: {e}")
else:
    print("⚠️  Hive table not available for quality analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Stream Control and Management

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⏹️ **Stop Streaming** (Run when ready to stop)

# COMMAND ----------

# Uncomment the next line to stop the stream
# listener.stop_streaming()

print("ℹ️  To stop streaming, uncomment and run the line above")
print("ℹ️  Stream will continue running until manually stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔧 **Stream Maintenance**

# COMMAND ----------

# Stream maintenance operations
print("🔧 STREAM MAINTENANCE OPTIONS")
print("-" * 40)

# Repair table partitions
if hive_manager.table_exists():
    try:
        repair_result = hive_manager.repair_table()
        print(f"🔨 Partition Repair: Added {repair_result.get('partitions_added', 0)} partitions")
    except Exception as e:
        print(f"⚠️  Partition repair: {e}")

# Validate bronze data
try:
    validation_result = bronze_handler.validate_bronze_data(sample_size=1000)
    print(f"✅ Data Validation: Checked {validation_result.get('sample_size', 0)} records")

    if 'checks' in validation_result:
        checks = validation_result['checks']
        print(f"   Null Record IDs: {checks.get('null_record_ids', 'N/A')}")
        print(f"   Invalid JSON: {checks.get('invalid_json_payloads', 'N/A')}")
except Exception as e:
    print(f"⚠️  Data validation: {e}")

# Update table statistics
if hive_manager.table_exists():
    try:
        hive_manager.update_table_statistics()
        print("📊 Table statistics updated")
    except Exception as e:
        print(f"⚠️  Statistics update: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Performance and Optimization

# COMMAND ----------

print("⚡ PERFORMANCE MONITORING")
print("-" * 40)

# Check current stream progress
if listener.stream_query and listener.stream_query.isActive:
    try:
        progress = listener.stream_query.lastProgress

        if progress:
            print(f"📊 Batch Details:")
            print(f"   Batch ID: {progress.get('id', 'N/A')}")
            print(f"   Timestamp: {progress.get('timestamp', 'N/A')}")
            print(f"   Input Rows/Sec: {progress.get('inputRowsPerSecond', 0):,.2f}")
            print(f"   Process Rows/Sec: {progress.get('processedRowsPerSecond', 0):,.2f}")
            print(f"   Duration (ms): {progress.get('durationMs', {}).get('triggerExecution', 'N/A')}")

            # Source statistics
            sources = progress.get('sources', [])
            for source in sources:
                print(f"   📡 Source: {source.get('description', 'EventHub')}")
                print(f"      Input Rows: {source.get('inputRowsPerSecond', 0):,.2f}/sec")
                print(f"      Processing Rate: {source.get('processedRowsPerSecond', 0):,.2f}/sec")

    except Exception as e:
        print(f"⚠️  Progress monitoring: {e}")

# Memory and resource usage
try:
    driver_memory = spark.conf.get("spark.driver.memory", "unknown")
    executor_memory = spark.conf.get("spark.executor.memory", "unknown")
    executor_cores = spark.conf.get("spark.executor.cores", "unknown")

    print(f"\n💾 Resource Configuration:")
    print(f"   Driver Memory: {driver_memory}")
    print(f"   Executor Memory: {executor_memory}")
    print(f"   Executor Cores: {executor_cores}")

except Exception as e:
    print(f"⚠️  Resource info: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 **Implementation Summary**
# MAGIC
# MAGIC This notebook implements the **Purchase Order Item EventHub Listener** with:
# MAGIC
# MAGIC ### ✅ **Features Implemented:**
# MAGIC - **Exact Widget Configuration**: Follows reference pattern from `EventHub_Listener_HiveMetastore_Databricks.py`
# MAGIC - **Class-Based Architecture**: Uses imported classes for modular design
# MAGIC - **Bronze Schema**: Complete technical metadata + business fields
# MAGIC - **ADLS Gen2 Integration**: Partitioned storage with proper path management
# MAGIC - **Hive Metastore**: Automatic table creation and maintenance
# MAGIC - **Structured Streaming**: Real-time processing with checkpointing
# MAGIC - **Data Quality Monitoring**: Built-in validation and quality checks
# MAGIC - **Performance Monitoring**: Real-time statistics and progress tracking
# MAGIC
# MAGIC ### 📊 **Bronze Layer Schema:**
# MAGIC - **Technical Metadata**: record_id, ingestion_timestamp, ingestion_date, ingestion_hour
# MAGIC - **EventHub Metadata**: partition, offset, sequence_number, enqueued_time
# MAGIC - **Processing Metadata**: cluster_id, consumer_group, eventhub_name
# MAGIC - **Payload**: raw_payload (JSON), payload_size_bytes
# MAGIC - **Business Fields**: order_id, customer_id, order_status, payment_status, total_amount, quality metrics
# MAGIC
# MAGIC ### 🏗️ **Infrastructure:**
# MAGIC - **Partitioning**: By ingestion_date and ingestion_hour
# MAGIC - **Storage Format**: Parquet with Snappy compression
# MAGIC - **Table Management**: Automatic creation, repair, and statistics
# MAGIC - **Error Handling**: Robust error handling with retry logic
# MAGIC - **Monitoring**: Real-time status and quality dashboards
# MAGIC
# MAGIC ### 🔗 **Integration Points:**
# MAGIC - **EventHub Source**: Consumes from purchase-order-items EventHub
# MAGIC - **Bronze Layer Output**: Writes to ADLS Gen2 with Hive registration
# MAGIC - **DQX Pipeline Ready**: Quality issues included for downstream processing
# MAGIC - **Silver Layer Ready**: Structured data available for transformation
# MAGIC
# MAGIC ### 📋 **Next Steps:**
# MAGIC 1. Monitor streaming progress in real-time
# MAGIC 2. Validate data quality and business metrics
# MAGIC 3. Execute Bronze to Silver DQX pipeline
# MAGIC 4. Set up automated monitoring and alerting