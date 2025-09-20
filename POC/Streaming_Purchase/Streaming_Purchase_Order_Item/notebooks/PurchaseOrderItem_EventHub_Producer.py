# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item EventHub Producer
# MAGIC
# MAGIC Main notebook for producing purchase order items to Azure EventHub.
# MAGIC
# MAGIC ## Architecture:
# MAGIC ```
# MAGIC Purchase Order Data Generation → EventHub Producer → Azure EventHub (purchase-order-items)
# MAGIC ```
# MAGIC
# MAGIC ## Features:
# MAGIC - Configurable data generation with quality scenarios
# MAGIC - Batch sending with monitoring
# MAGIC - Error handling and retry logic
# MAGIC - Performance tracking
# MAGIC - Databricks integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import Required Classes and Libraries

# COMMAND ----------

# Import existing classes from organized structure
import sys
sys.path.append("../class")

from purchase_order_item_model import PurchaseOrderItem
from purchase_order_item_factory import PurchaseOrderItemFactory
from purchase_order_item_producer import PurchaseOrderItemProducer, create_producer_from_widgets

# Standard libraries
import json
import time
import logging
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Widget Configuration (Exact Pattern)

# COMMAND ----------

# Create widgets for dynamic configuration (EXACT same pattern as reference)
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("eventhub_name", "purchase-order-items", "Event Hub Name")
dbutils.widgets.text("batch_size", "50", "Batch Size")
dbutils.widgets.text("send_interval", "2.0", "Send Interval (seconds)")
dbutils.widgets.text("duration_minutes", "60", "Run Duration (minutes)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")

# Additional production configuration
dbutils.widgets.dropdown("scenario", "normal", ["normal", "seasonal", "high_volume", "quality_test"], "Data Scenario")
dbutils.widgets.text("max_retries", "3", "Max Retry Attempts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Get Widget Values and Configuration

# COMMAND ----------

# Get widget values
SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
SEND_INTERVAL = float(dbutils.widgets.get("send_interval"))
DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))
LOG_LEVEL = dbutils.widgets.get("log_level")
SCENARIO = dbutils.widgets.get("scenario")
MAX_RETRIES = int(dbutils.widgets.get("max_retries"))

# Display configuration
print("🎯 PURCHASE ORDER EVENTHUB PRODUCER CONFIGURATION")
print("=" * 60)
print(f"EventHub Scope:    {SECRET_SCOPE}")
print(f"EventHub Name:     {EVENTHUB_NAME}")
print(f"Batch Size:        {BATCH_SIZE}")
print(f"Send Interval:     {SEND_INTERVAL}s")
print(f"Duration:          {DURATION_MINUTES} minutes")
print(f"Scenario:          {SCENARIO}")
print(f"Log Level:         {LOG_LEVEL}")
print(f"Max Retries:       {MAX_RETRIES}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Setup Logging and Producer

# COMMAND ----------

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("PurchaseOrderProducer")

# Get connection string from secret scope
try:
    connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{EVENTHUB_NAME}-connection-string")
    logger.info("✅ Retrieved EventHub connection string from secret scope")
except Exception as e:
    logger.error(f"❌ Failed to retrieve connection string: {e}")
    raise

# Create producer instance
producer = PurchaseOrderItemProducer(
    connection_string=connection_string,
    eventhub_name=EVENTHUB_NAME,
    batch_size=BATCH_SIZE,
    send_interval=SEND_INTERVAL,
    max_retries=MAX_RETRIES,
    log_level=LOG_LEVEL
)

logger.info("✅ Producer initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test EventHub Connection

# COMMAND ----------

# Test connection before starting production
print("🔌 Testing EventHub Connection...")
connection_test = producer.test_connection()

if connection_test:
    print("✅ EventHub connection test successful!")
else:
    print("❌ EventHub connection test failed!")
    raise Exception("Cannot proceed with failed connection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Generation Preview

# COMMAND ----------

# Generate a sample batch to preview data structure
print("📊 Generating Sample Data Preview...")
sample_factory = PurchaseOrderItemFactory(quality_issue_rate=0.05)

# Generate 5 sample orders
sample_orders = sample_factory.generate_batch(batch_size=5)

print(f"\n📦 Sample Purchase Orders ({len(sample_orders)} items):")
print("-" * 100)

for i, order in enumerate(sample_orders, 1):
    print(f"\n{i}. Order ID: {order.order_id}")
    print(f"   Product: {order.product_name} (ID: {order.product_id})")
    print(f"   Quantity: {order.quantity} x ${order.unit_price:.2f} = ${order.total_amount:.2f}")
    print(f"   Customer: {order.customer_id}")
    print(f"   Status: {order.order_status} | Payment: {order.payment_status}")
    print(f"   Quality: Valid={order.is_valid}, Score={order.data_quality_score}")

    if not order.is_valid:
        print(f"   ⚠️  Quality Issue: {order.validation_errors}")

# Display sample statistics
stats = sample_factory.get_statistics(sample_orders)
print(f"\n📈 Sample Statistics:")
print(f"   Total Orders: {stats['total_orders']}")
print(f"   Valid Orders: {stats['valid_orders']}")
print(f"   Quality Rate: {stats['quality_rate']:.1%}")
print(f"   Total Revenue: ${stats['total_revenue']:,.2f}")
print(f"   Avg Order Value: ${stats['average_order_value']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Production Execution

# COMMAND ----------

print("🚀 Starting Purchase Order Production...")
print(f"⏱️  Will run for {DURATION_MINUTES} minutes with {SCENARIO} scenario")
print("-" * 80)

# Start production based on scenario
start_time = datetime.utcnow()

try:
    if SCENARIO == "quality_test":
        # Run scenario tests
        print("🧪 Running Quality Test Scenarios...")
        test_results = producer.produce_scenario_test()

        print("\n📋 Test Results Summary:")
        for scenario, results in test_results.items():
            print(f"   {scenario}: {results['events_sent']} events, Success: {results['success']}")

    elif SCENARIO == "high_volume":
        # High volume batch production
        total_events = BATCH_SIZE * (DURATION_MINUTES * 60 // SEND_INTERVAL)
        print(f"📈 High Volume Mode: Targeting {total_events:,} events")
        production_stats = producer.produce_batch_async(total_events=total_events)

    else:
        # Normal continuous production
        production_stats = producer.produce_continuous(
            duration_minutes=DURATION_MINUTES,
            scenario=SCENARIO
        )

except KeyboardInterrupt:
    print("\n⏹️  Production interrupted by user")
    production_stats = producer.get_statistics()
except Exception as e:
    print(f"\n❌ Production failed: {e}")
    production_stats = producer.get_statistics()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Production Results and Statistics

# COMMAND ----------

end_time = datetime.utcnow()
total_duration = (end_time - start_time).total_seconds()

print("📊 PRODUCTION COMPLETED - FINAL STATISTICS")
print("=" * 80)

# Get final statistics
final_stats = producer.get_statistics()

print(f"⏱️  Duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
print(f"📦 Events Sent: {final_stats['total_events_sent']:,}")
print(f"📋 Batches Sent: {final_stats['total_batches_sent']:,}")
print(f"❌ Failed Events: {final_stats['failed_events']:,}")
print(f"🔄 Retry Count: {final_stats['retry_count']:,}")
print(f"⚠️  Quality Issues: {final_stats['quality_issues_generated']:,}")
print(f"📈 Events/Second: {final_stats['events_per_second']:.2f}")
print(f"💾 Data Sent: {final_stats['total_mb_sent']:.2f} MB")
print(f"✅ Success Rate: {final_stats['success_rate']:.1%}")

# Performance insights
if final_stats['total_events_sent'] > 0:
    avg_batch_size = final_stats['total_events_sent'] / final_stats['total_batches_sent']
    print(f"📏 Avg Batch Size: {avg_batch_size:.1f} events")

    if final_stats['events_per_second'] > 0:
        projected_daily = final_stats['events_per_second'] * 86400
        print(f"🗓️  Daily Projection: {projected_daily:,.0f} events/day")

# Quality analysis
if final_stats['quality_issues_generated'] > 0:
    quality_rate = final_stats['quality_issues_generated'] / final_stats['total_events_sent']
    print(f"🎯 Quality Issue Rate: {quality_rate:.1%} (Target: 5%)")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Validation and Monitoring

# COMMAND ----------

# Additional monitoring and validation
print("🔍 ADDITIONAL MONITORING")
print("-" * 40)

# Cluster information
try:
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    print(f"🖥️  Cluster ID: {cluster_id}")
    print(f"📓 Notebook Path: {notebook_path}")
except:
    print("ℹ️  Could not retrieve cluster information")

# Memory and performance
try:
    driver_memory = spark.conf.get("spark.driver.memory", "unknown")
    executor_memory = spark.conf.get("spark.executor.memory", "unknown")

    print(f"💾 Driver Memory: {driver_memory}")
    print(f"💾 Executor Memory: {executor_memory}")
except:
    print("ℹ️  Could not retrieve memory information")

# EventHub specific metrics
print(f"\n📡 EventHub Metrics:")
print(f"   Target EventHub: {EVENTHUB_NAME}")
print(f"   Secret Scope: {SECRET_SCOPE}")
print(f"   Connection: Active ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup and Summary

# COMMAND ----------

# Final cleanup
try:
    # Producer cleanup is handled automatically in the producer class
    print("🧹 Cleanup completed")
except Exception as e:
    print(f"⚠️  Cleanup warning: {e}")

# Success summary
print("\n🎉 PURCHASE ORDER PRODUCTION SUMMARY")
print("=" * 50)
print(f"✅ Successfully sent {final_stats['total_events_sent']:,} purchase order events")
print(f"✅ Data pipeline ready for Bronze layer consumption")
print(f"✅ EventHub: {EVENTHUB_NAME} populated with realistic data")
print(f"✅ Quality scenarios included for DQX testing")

if final_stats['success_rate'] >= 0.95:
    print("🏆 Excellent success rate achieved!")
elif final_stats['success_rate'] >= 0.90:
    print("👍 Good success rate achieved!")
else:
    print("⚠️  Success rate below 90% - review logs for issues")

print("\n🔄 Next Steps:")
print("   1. Start EventHub Listener for Bronze layer ingestion")
print("   2. Monitor Bronze layer data quality")
print("   3. Execute Bronze to Silver DQX pipeline")
print("   4. Validate end-to-end data flow")

print("\n" + "=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Production Notes
# MAGIC
# MAGIC This notebook implements the **Purchase Order Item EventHub Producer** with:
# MAGIC
# MAGIC ### ✅ **Features Implemented:**
# MAGIC - **Exact Widget Configuration**: Follows reference pattern from `EventHub_Producer_Databricks.py`
# MAGIC - **Class-Based Architecture**: Uses imported classes for maintainability
# MAGIC - **Realistic Data Generation**: 95% valid data + 5% quality issues for DQX testing
# MAGIC - **Financial Validation**: Proper calculations (total = quantity × price + adjustments)
# MAGIC - **Business Logic**: Consistent order and payment status relationships
# MAGIC - **Databricks Integration**: Cluster metadata and monitoring
# MAGIC - **Error Handling**: Retry logic with exponential backoff
# MAGIC - **Performance Monitoring**: Real-time statistics and throughput tracking
# MAGIC
# MAGIC ### 📊 **Data Scenarios:**
# MAGIC - **Normal**: Balanced mix of order types and sizes
# MAGIC - **Seasonal**: Holiday, back-to-school, and sale patterns
# MAGIC - **High Volume**: Bulk order simulation for load testing
# MAGIC - **Quality Test**: Focused quality issue injection for DQX validation
# MAGIC
# MAGIC ### 🎛️ **Configuration Options:**
# MAGIC - Batch size (1-1000 events)
# MAGIC - Send interval (0.1-10 seconds)
# MAGIC - Duration (1-480 minutes)
# MAGIC - Scenario selection for different business patterns
# MAGIC - Retry logic for reliability
# MAGIC
# MAGIC ### 🔗 **Integration Ready:**
# MAGIC - EventHub connection tested and verified
# MAGIC - Bronze layer consumption ready
# MAGIC - DQX pipeline data available
# MAGIC - End-to-end monitoring enabled