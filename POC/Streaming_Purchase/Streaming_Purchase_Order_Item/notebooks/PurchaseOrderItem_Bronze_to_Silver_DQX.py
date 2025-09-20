# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Order Item Bronze to Silver DQX Pipeline
# MAGIC
# MAGIC **Class-Based Bronze to Silver Streaming Pipeline with Databricks DQX Quality Framework**
# MAGIC
# MAGIC This notebook implements a comprehensive DQX quality validation pipeline for Purchase Order Items using:
# MAGIC - **Databricks DQX Framework** integration for data quality validation
# MAGIC - **Single Enhanced Table Approach** - ALL records (valid + invalid) with quality flags
# MAGIC - Comprehensive quality rule checks (financial validation, business logic, format validation)
# MAGIC - Real-time streaming with quality monitoring and lineage tracking
# MAGIC - Class-based architecture for maintainability and reusability
# MAGIC
# MAGIC **Enhanced Architecture:**
# MAGIC ```
# MAGIC Bronze Layer → JSON Parsing → DQX Quality Checks → Single Enhanced Silver Table (ALL records with quality flags)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Widgets

# COMMAND ----------

# Configuration widgets following exact pattern from reference pipeline
# Common properties aligned with EventHub_Listener_HiveMetastore_Databricks
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("storage_account", "idisitcusadls2", "Storage Account Name")
dbutils.widgets.text("container", "purchase-order-test", "ADLS Container Name")

# Source configuration - Bronze layer
dbutils.widgets.text("bronze_database", "bronze", "Bronze Database Name")
dbutils.widgets.text("bronze_table", "purchase_order_items_raw", "Bronze Table Name")

# Target configuration - Single enhanced Silver layer with DQX quality metadata
dbutils.widgets.text("silver_database", "silver", "Silver Database Name")
dbutils.widgets.text("silver_table", "purchase_order_items_dqx", "Silver Table Name")

# Pipeline paths
dbutils.widgets.text("silver_path", "/mnt/silver/purchase_orders", "Silver Layer Path")
dbutils.widgets.text("checkpoint_path", "/mnt/checkpoints/purchase-order-dqx", "DQX Pipeline Checkpoint Path")

# DQX Quality configuration
dbutils.widgets.dropdown("quality_criticality", "error", ["error", "warn"], "DQX Quality Criticality Level")
dbutils.widgets.text("quality_threshold", "0.95", "Quality Pass Threshold (0-1)")

# Streaming configuration
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

QUALITY_CRITICALITY = dbutils.widgets.get("quality_criticality")
QUALITY_THRESHOLD = float(dbutils.widgets.get("quality_threshold"))
TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
MAX_EVENTS_PER_TRIGGER = int(dbutils.widgets.get("max_events_per_trigger"))

print("🔧 Purchase Order Bronze to Silver DQX Pipeline Configuration:")
print(f"   Secret Scope: {SECRET_SCOPE}")
print(f"   Storage Account: {STORAGE_ACCOUNT}")
print(f"   Container: {CONTAINER}")
print(f"   Source: {BRONZE_FULL_TABLE}")
print(f"   Enhanced Silver Target: {SILVER_FULL_TABLE} (Single table with DQX quality flags)")
print(f"   DQX Quality Criticality: {QUALITY_CRITICALITY}")
print(f"   Quality Threshold: {QUALITY_THRESHOLD}")
print(f"   Trigger: {TRIGGER_MODE}")
print(f"   Max Events/Trigger: {MAX_EVENTS_PER_TRIGGER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Classes

# COMMAND ----------

import sys
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project path for imports
current_path = os.path.dirname(os.path.abspath(__file__)).replace('/notebooks', '')
sys.path.insert(0, current_path)

# Import our custom classes
try:
    from class.purchase_order_dqx_rules import PurchaseOrderDQXRules, DQXRuleConfig
    from class.purchase_order_dqx_pipeline import PurchaseOrderDQXPipeline, PipelineConfig
    from class.purchase_order_silver_manager import PurchaseOrderSilverManager, SilverTableConfig
    from class.purchase_order_dqx_monitor import PurchaseOrderDQXMonitor, MonitorConfig

    print("✅ Successfully imported all Purchase Order DQX classes")

except ImportError as e:
    print(f"❌ Error importing classes: {e}")
    print("📁 Current path:", current_path)
    print("📂 Available files:", os.listdir(os.path.join(current_path, 'class')))
    raise

# Standard PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("✅ All libraries and classes imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize Configuration Objects

# COMMAND ----------

# Initialize configuration objects for our classes
print("🔧 Initializing configuration objects...")

# DQX Rules Configuration
dqx_rule_config = DQXRuleConfig(
    criticality=QUALITY_CRITICALITY,
    quality_threshold=QUALITY_THRESHOLD,
    enable_financial_rules=True,
    enable_format_rules=True,
    enable_range_rules=True,
    enable_consistency_rules=True
)

# Pipeline Configuration
pipeline_config = PipelineConfig(
    bronze_database=BRONZE_DATABASE,
    bronze_table=BRONZE_TABLE,
    silver_database=SILVER_DATABASE,
    silver_table=SILVER_TABLE,
    silver_path=SILVER_PATH,
    checkpoint_path=CHECKPOINT_PATH,
    storage_account=STORAGE_ACCOUNT,
    container=CONTAINER,
    quality_criticality=QUALITY_CRITICALITY,
    quality_threshold=QUALITY_THRESHOLD,
    trigger_mode=TRIGGER_MODE,
    max_events_per_trigger=MAX_EVENTS_PER_TRIGGER
)

# Silver Table Configuration
silver_table_config = SilverTableConfig(
    database=SILVER_DATABASE,
    table=SILVER_TABLE,
    storage_account=STORAGE_ACCOUNT,
    container=CONTAINER,
    silver_path=SILVER_PATH,
    quality_criticality=QUALITY_CRITICALITY,
    quality_threshold=QUALITY_THRESHOLD
)

# Monitor Configuration
monitor_config = MonitorConfig(
    refresh_interval=30,
    dashboard_auto_refresh=True,
    alert_threshold=0.8,
    max_failure_samples=10
)

print("✅ Configuration objects initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Class Instances

# COMMAND ----------

print("🏗️ Initializing class instances...")

# Initialize DQX Rules
print("📋 Initializing DQX quality rules...")
dqx_rules = PurchaseOrderDQXRules(config=dqx_rule_config)
quality_rules = dqx_rules.get_rules()

print(f"✅ DQX Rules initialized: {len(quality_rules)} rules defined")
print(f"   Rule summary: {dqx_rules.get_rule_summary()}")

# Initialize Silver Manager
print("🗄️ Initializing Silver table manager...")
silver_manager = PurchaseOrderSilverManager(spark, silver_table_config)

print(f"✅ Silver Manager initialized for table: {SILVER_FULL_TABLE}")

# Initialize DQX Pipeline
print("🚀 Initializing DQX pipeline...")
dqx_pipeline = PurchaseOrderDQXPipeline(spark, pipeline_config)

print(f"✅ DQX Pipeline initialized")
print(f"   Source: {BRONZE_FULL_TABLE}")
print(f"   Target: {SILVER_FULL_TABLE}")

# Initialize Monitor
print("📊 Initializing DQX monitor...")
dqx_monitor = PurchaseOrderDQXMonitor(spark, SILVER_FULL_TABLE, monitor_config)

print("✅ All class instances initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Setup Database and Enhanced Silver Table

# COMMAND ----------

print("🏗️ Setting up database and enhanced Silver table with DQX support...")

# Create database if not exists
print(f"📝 Creating database: {SILVER_DATABASE}")
silver_manager.create_database_if_not_exists()

# Create enhanced Silver table with DQX metadata
print(f"📝 Creating enhanced Silver table: {SILVER_FULL_TABLE}")
table_creation_success = silver_manager.create_enhanced_silver_table()

if table_creation_success:
    print("✅ Enhanced Silver table setup completed successfully")
    print(f"📊 Table: {SILVER_FULL_TABLE}")
    print("   Approach: Single table with quality flags (flag_check: PASS/FAIL)")

    # Show table information
    silver_manager.show_table_info()

    # Display schema information
    schema_info = silver_manager.get_schema_info()
    print(f"\n📋 Schema Information:")
    print(f"   Total Fields: {schema_info['total_fields']}")
    print(f"   Business Fields: {schema_info['business_fields']}")
    print(f"   Technical Fields: {schema_info['technical_fields']}")
    print(f"   DQX Quality Fields: {schema_info['dqx_fields']}")
    print(f"   Partition Fields: {schema_info['partition_fields']}")

else:
    print("❌ Enhanced Silver table setup failed")
    raise Exception("Could not create Silver table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Start the Enhanced DQX Streaming Pipeline

# COMMAND ----------

print("🚀 Starting Purchase Order Bronze to Silver DQX Streaming Pipeline")
print("=" * 70)

# Validate rules configuration
if not dqx_rules.validate_rule_configuration():
    print("⚠️ Warning: DQX rules validation failed, proceeding with basic validation")

# Start the streaming pipeline
try:
    print("🔄 Starting DQX streaming pipeline...")
    streaming_query = dqx_pipeline.start_streaming(quality_rules)

    if streaming_query:
        print("✅ DQX streaming pipeline started successfully!")

        # Get pipeline status
        status = dqx_pipeline.get_pipeline_status()
        print(f"\n📊 Pipeline Status:")
        for key, value in status.items():
            print(f"   {key}: {value}")

        print(f"\n🔍 Quality Framework: {'Databricks DQX' if dqx_pipeline.dqx_available else 'Basic Validation'}")
        print(f"📋 Quality Rules: {len(quality_rules)} rules applied")
        print(f"🎯 Quality Approach: Single enhanced table with quality flags")

    else:
        print("❌ Failed to start streaming pipeline")

except Exception as e:
    print(f"❌ Error starting pipeline: {e}")
    streaming_query = None
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Monitor the DQX Pipeline

# COMMAND ----------

if streaming_query:
    print("📊 Monitoring Purchase Order Bronze to Silver DQX Pipeline")
    print("=" * 60)

    try:
        # Initial dashboard display
        print("🔄 Displaying initial DQX dashboard...")
        dqx_monitor.display_quality_dashboard(streaming_query)

        # Monitor for a few iterations
        monitoring_iterations = 3
        monitoring_interval = 30  # seconds

        print(f"\n🔍 Starting {monitoring_iterations} monitoring iterations...")

        for i in range(monitoring_iterations):
            if streaming_query.isActive:
                print(f"\n🔄 DQX Monitoring Iteration {i+1}/{monitoring_iterations}")
                print(f"   Time: {datetime.now().strftime('%H:%M:%S')}")

                # Display dashboard
                dqx_monitor.display_quality_dashboard(streaming_query)

                # Show progress details
                try:
                    progress = streaming_query.lastProgress
                    if progress:
                        batch_id = progress.get('batchId', 0)
                        input_rate = progress.get('inputRowsPerSecond', 0)
                        processing_rate = progress.get('processedRowsPerSecond', 0)

                        print(f"   📊 Batch ID: {batch_id}")
                        print(f"   📈 Input Rate: {input_rate:.1f} rows/sec")
                        print(f"   ⚡ Processing Rate: {processing_rate:.1f} rows/sec")
                except Exception as e:
                    print(f"   ⚠️ Progress info unavailable: {e}")

                # Wait before next iteration
                if i < monitoring_iterations - 1:
                    print(f"   ⏱️ Waiting {monitoring_interval} seconds...")
                    time.sleep(monitoring_interval)
            else:
                print("❌ Streaming pipeline is not active")
                break

        # Final status check
        if streaming_query.isActive:
            print("\n✅ DQX pipeline monitoring completed - pipeline running in background")

            # Show quality analysis
            print("\n📊 Performing quality analysis...")
            dqx_monitor.analyze_quality_failures(limit=5)

            # Show DQX lineage tracking
            print("\n🔗 DQX lineage tracking...")
            dqx_monitor.track_dqx_lineage(limit=5)

            # Get monitoring summary
            summary = dqx_monitor.get_monitoring_summary()
            print(f"\n📈 Monitoring Summary:")
            for key, value in summary.items():
                if key != 'timestamp':
                    print(f"   {key}: {value}")

        else:
            print("\n⚠️ Streaming pipeline stopped during monitoring")

    except KeyboardInterrupt:
        print("\n⏹️ Monitoring interrupted by user")
    except Exception as e:
        print(f"\n❌ Monitoring error: {e}")

else:
    print("❌ No streaming query available for monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pipeline Management Commands

# COMMAND ----------

def check_dqx_pipeline_status():
    """Check current DQX pipeline status"""
    if streaming_query:
        status = dqx_pipeline.get_pipeline_status()
        print(f"📊 Purchase Order DQX Pipeline Status:")
        for key, value in status.items():
            print(f"   {key}: {value}")

        # Additional monitoring summary
        summary = dqx_monitor.get_monitoring_summary()
        if 'error' not in summary:
            print(f"\n📈 Quality Summary:")
            print(f"   Quality Rate: {summary.get('quality_rate', 0):.2%}")
            print(f"   Quality Status: {summary.get('quality_status', 'unknown')}")
            print(f"   Total Records: {summary.get('total_records', 0):,}")

        return True
    else:
        print("❌ No DQX pipeline found")
        return False

def stop_dqx_pipeline():
    """Stop the DQX pipeline gracefully"""
    if streaming_query:
        print("⏹️ Stopping Purchase Order DQX pipeline...")
        dqx_pipeline.stop_streaming()

        # Final quality analysis
        print("\n📊 Final quality analysis...")
        dqx_monitor.analyze_quality_failures()
    else:
        print("ℹ️ No active pipeline to stop")

def show_quality_trends():
    """Show quality trends analysis"""
    print("📈 Analyzing quality trends...")
    dqx_monitor.analyze_quality_trends(days=7)

def show_silver_table_stats():
    """Show Silver table statistics"""
    print("📊 Enhanced Silver Table Statistics:")
    silver_manager.show_table_info()

    # Additional quality analysis
    print("\n🔍 Quality Failure Analysis:")
    silver_manager.analyze_quality_failures(limit=5)

def optimize_silver_table():
    """Optimize the Silver table for better performance"""
    print("🔧 Optimizing Silver table...")
    silver_manager.optimize_table()

# Execute status check
print("🔍 Current Pipeline Status:")
check_dqx_pipeline_status()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Advanced Quality Analysis

# COMMAND ----------

def perform_comprehensive_quality_analysis():
    """Perform comprehensive quality analysis"""

    print("🔍 Comprehensive Quality Analysis")
    print("=" * 50)

    # 1. Overall statistics
    stats = silver_manager.get_table_statistics()
    if 'error' not in stats:
        print("📊 Table Statistics:")
        print(f"   Total Records: {stats['record_counts']['total']:,}")

        if 'quality_distribution' in stats:
            print(f"   Quality Distribution:")
            for status, count in stats['quality_distribution'].items():
                print(f"     {status}: {count:,}")

        if 'quality_rate' in stats:
            print(f"   Overall Quality Rate: {stats['quality_rate']:.2%}")

    # 2. Quality trends
    print(f"\n📈 Quality Trends (Last 7 Days):")
    dqx_monitor.analyze_quality_trends(days=7)

    # 3. Failure analysis
    print(f"\n🚫 Quality Failures Analysis:")
    dqx_monitor.analyze_quality_failures(limit=10)

    # 4. DQX lineage tracking
    print(f"\n🔗 DQX Lineage Tracking:")
    dqx_monitor.track_dqx_lineage(limit=10)

    # 5. Business insights
    print(f"\n💼 Business Insights:")

    try:
        # Most common failure patterns
        business_analysis = spark.table(SILVER_FULL_TABLE).filter("flag_check = 'FAIL'")
        failure_count = business_analysis.count()

        if failure_count > 0:
            print(f"   Failed Orders Analysis:")

            # By product category
            print(f"   📊 Failures by Product Category:")
            business_analysis.groupBy("product_category").count() \
                .orderBy(col("count").desc()).show(5)

            # By order status
            print(f"   📊 Failures by Order Status:")
            business_analysis.groupBy("order_status").count() \
                .orderBy(col("count").desc()).show(5)

            # Financial issues
            financial_failures = business_analysis.filter(
                col("description_failure").contains("total_amount") |
                col("description_failure").contains("calculation")
            ).count()

            print(f"   💰 Financial Calculation Issues: {financial_failures}")

        else:
            print("   ✅ No quality failures - excellent data quality!")

    except Exception as e:
        print(f"   ⚠️ Business analysis error: {e}")

# Execute comprehensive analysis
perform_comprehensive_quality_analysis()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Final Summary and Documentation

# COMMAND ----------

def display_pipeline_summary():
    """Display comprehensive pipeline summary"""

    summary_html = f"""
    <div style="border: 2px solid #4CAF50; border-radius: 15px; padding: 20px; background: #f8f9fa;">
        <h2 style="color: #4CAF50; text-align: center;">🔍 Purchase Order DQX Pipeline Summary</h2>

        <div style="background: #d4edda; padding: 15px; border-radius: 10px; border-left: 5px solid #28a745; margin: 20px 0;">
            <h3 style="color: #155724; margin-top: 0;">📈 Class-Based Architecture Implementation</h3>
            <p style="margin: 0; color: #155724;">This pipeline uses <strong>proper class organization</strong> with separation of concerns</p>
        </div>

        <div style="background: #cce5ff; padding: 15px; border-radius: 10px; border-left: 5px solid #0066cc; margin: 20px 0;">
            <h3 style="color: #003d7a; margin-top: 0;">🎯 Single Enhanced Table Approach</h3>
            <p style="margin: 0; color: #003d7a;"><strong>ALL records</strong> (valid + invalid) in: <code>{SILVER_FULL_TABLE}</code></p>
            <p style="margin: 5px 0; color: #003d7a;">Quality tracking via <strong>flag_check</strong> field: PASS/FAIL</p>
        </div>

        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin: 20px 0;">
            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #2196F3;">
                <h3 style="color: #2196F3; margin-top: 0;">🏗️ Classes Implemented</h3>
                <ul style="margin: 0;">
                    <li><strong>PurchaseOrderDQXRules:</strong> Quality rules definition</li>
                    <li><strong>PurchaseOrderDQXPipeline:</strong> Streaming transformation</li>
                    <li><strong>PurchaseOrderSilverManager:</strong> Table management</li>
                    <li><strong>PurchaseOrderDQXMonitor:</strong> Quality monitoring</li>
                </ul>
            </div>

            <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #FF9800;">
                <h3 style="color: #FF9800; margin-top: 0;">📊 Configuration</h3>
                <ul style="margin: 0;">
                    <li><strong>Storage:</strong> {STORAGE_ACCOUNT}/{CONTAINER}</li>
                    <li><strong>Quality Framework:</strong> {'Databricks DQX' if dqx_pipeline.dqx_available else 'Basic Validation'}</li>
                    <li><strong>Quality Rules:</strong> {len(quality_rules)} validations</li>
                    <li><strong>Trigger:</strong> {TRIGGER_MODE}</li>
                    <li><strong>Threshold:</strong> {QUALITY_THRESHOLD:.1%}</li>
                </ul>
            </div>
        </div>

        <div style="background: white; padding: 15px; border-radius: 10px; border-left: 5px solid #9C27B0; margin: 20px 0;">
            <h3 style="color: #9C27B0; margin-top: 0;">✨ DQX Quality Rules Categories</h3>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px;">
                <div>
                    <h4>Critical Rules (Error Level):</h4>
                    <ul style="margin: 5px 0; font-size: 14px;">
                        <li>Financial accuracy (total = quantity × price)</li>
                        <li>Positive values validation</li>
                        <li>Required fields validation</li>
                        <li>Tax calculation validation</li>
                    </ul>
                </div>
                <div>
                    <h4>Warning Rules:</h4>
                    <ul style="margin: 5px 0; font-size: 14px;">
                        <li>Status validation (order/payment)</li>
                        <li>Business logic consistency</li>
                        <li>Format validation (ID patterns)</li>
                        <li>Range validation (reasonable limits)</li>
                    </ul>
                </div>
            </div>
        </div>

        <div style="background: #fff3e0; padding: 15px; border-radius: 10px; border-left: 5px solid #FF9800; margin: 20px 0;">
            <h3 style="color: #e65100; margin-top: 0;">📋 Usage Examples</h3>
            <div style="font-family: monospace; background: #f5f5f5; padding: 10px; border-radius: 5px;">
                <div style="margin-bottom: 10px;">
                    <strong>Get valid records:</strong><br>
                    <code>SELECT * FROM {SILVER_FULL_TABLE} WHERE flag_check = 'PASS'</code>
                </div>
                <div style="margin-bottom: 10px;">
                    <strong>Analyze failures:</strong><br>
                    <code>SELECT description_failure, COUNT(*) FROM {SILVER_FULL_TABLE} WHERE flag_check = 'FAIL' GROUP BY description_failure</code>
                </div>
                <div>
                    <strong>Quality metrics:</strong><br>
                    <code>SELECT flag_check, COUNT(*), AVG(dqx_quality_score) FROM {SILVER_FULL_TABLE} GROUP BY flag_check</code>
                </div>
            </div>
        </div>
    </div>
    """

    try:
        displayHTML(summary_html)
    except NameError:
        # Fallback to console output
        print("📊 PURCHASE ORDER DQX PIPELINE SUMMARY")
        print("=" * 50)
        print(f"🎯 Target: {SILVER_FULL_TABLE}")
        print(f"🏗️ Architecture: Class-based with single enhanced table")
        print(f"📋 Quality Rules: {len(quality_rules)} DQX validations")
        print(f"⚡ Framework: {'Databricks DQX' if dqx_pipeline.dqx_available else 'Basic Validation'}")
        print(f"🔧 Quality Threshold: {QUALITY_THRESHOLD:.1%}")
        print("✅ Implementation: Proper class separation and organization")

def show_class_organization():
    """Show the class organization implemented"""

    print("🏗️ CLASS-BASED ARCHITECTURE ORGANIZATION")
    print("=" * 50)
    print("📁 File Structure:")
    print("   class/")
    print("   ├── purchase_order_dqx_rules.py      - Quality rules definition")
    print("   ├── purchase_order_dqx_pipeline.py   - Streaming transformation logic")
    print("   ├── purchase_order_silver_manager.py - Table and schema management")
    print("   └── purchase_order_dqx_monitor.py    - Monitoring and dashboards")
    print("   ")
    print("   notebooks/")
    print("   └── PurchaseOrderItem_Bronze_to_Silver_DQX.py - Main execution notebook")
    print("")
    print("🎯 Benefits of Class Organization:")
    print("   ✅ Separation of concerns")
    print("   ✅ Reusable components")
    print("   ✅ Testable modules")
    print("   ✅ Maintainable codebase")
    print("   ✅ Type safety with dataclasses")
    print("   ✅ Proper error handling")
    print("   ✅ Comprehensive logging")

# Display final summary
display_pipeline_summary()
print()
show_class_organization()

# Final status check
print(f"\n🔍 Final Pipeline Status:")
final_status = check_dqx_pipeline_status()

if final_status and streaming_query and streaming_query.isActive:
    print(f"\n✅ Purchase Order DQX Pipeline is running successfully!")
    print(f"📊 Monitor the pipeline using the provided dashboard and analysis functions")
    print(f"⏹️ Use stop_dqx_pipeline() to stop the pipeline when needed")
else:
    print(f"\n⚠️ Pipeline status check - ensure Bronze table has data and pipeline is configured correctly")