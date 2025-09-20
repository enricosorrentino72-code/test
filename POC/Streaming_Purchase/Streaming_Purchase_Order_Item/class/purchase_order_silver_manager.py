"""
Purchase Order Silver Manager Module

This module manages the Silver layer Hive Metastore tables for purchase order items
with DQX quality metadata. It handles table creation, schema management, and ADLS
integration for the enhanced single table approach.

Author: DataOps Team
Date: 2024
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class SilverTableConfig:
    """Configuration for Silver table management"""
    database: str
    table: str
    storage_account: str
    container: str
    silver_path: str
    quality_criticality: str = "error"
    quality_threshold: float = 0.95


class PurchaseOrderSilverManager:
    """
    Manage Silver layer Hive Metastore tables for purchase order items with DQX metadata.

    This class handles the creation and management of enhanced Silver tables that store
    all records (valid and invalid) with quality flags in a single table approach.
    """

    def __init__(self, spark: SparkSession, config: SilverTableConfig):
        """
        Initialize the Silver table manager.

        Args:
            spark: Active Spark session
            config: Silver table configuration
        """
        self.spark = spark
        self.config = config
        self.full_table = f"{config.database}.{config.table}"

        # Configure ADLS paths
        self._setup_adls_configuration()

        # Define enhanced schema
        self.enhanced_silver_schema = self._get_enhanced_silver_schema()

        logger.info(f"✅ PurchaseOrderSilverManager initialized")
        logger.info(f"   Table: {self.full_table}")
        logger.info(f"   Storage: {config.storage_account}/{config.container}")

    def _setup_adls_configuration(self):
        """Setup ADLS Gen2 configuration and paths"""

        self.base_path = f"abfss://{self.config.container}@{self.config.storage_account}.dfs.core.windows.net"

        # Check if mount point exists
        try:
            self.spark.sql("SHOW DATABASES")  # Simple check to see if we can access
            # Try to check mount point
            mount_path = "/mnt/datalake/"
            try:
                # This would work in Databricks environment
                dbutils.fs.ls(mount_path)
                self.is_mounted = True
                logger.info("✅ ADLS Gen2 storage is mounted")
            except:
                self.is_mounted = False
                logger.info("⚠️ ADLS Gen2 storage mount not found - using direct abfss:// paths")
        except:
            self.is_mounted = False

    def get_table_path(self) -> str:
        """
        Get the full storage path for the Silver table.

        Returns:
            Full ADLS path for the table
        """
        if self.is_mounted:
            return self.config.silver_path
        else:
            # Convert mount path to direct ADLS path
            relative_path = self.config.silver_path.replace("/mnt/datalake/", "").replace("/mnt/", "")
            return f"{self.base_path}/{relative_path}"

    def _get_enhanced_silver_schema(self) -> StructType:
        """
        Define enhanced Silver layer schema with DQX quality metadata.

        Returns:
            StructType schema for the enhanced Silver table
        """
        return StructType([
            # Business Purchase Order Data
            StructField("order_id", StringType(), False),  # Business identifier
            StructField("product_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("discount_percentage", DecimalType(5, 2), True),
            StructField("tax_amount", DecimalType(10, 2), True),
            StructField("order_status", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("ship_date", TimestampType(), True),
            StructField("delivery_date", TimestampType(), True),
            StructField("customer_email", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("billing_address", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("warehouse_id", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("tracking_number", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("purchase_cluster_id", StringType(), True),
            StructField("notebook_path", StringType(), True),

            # Inherited Technical Fields from Bronze
            StructField("source_record_id", StringType(), False),  # Original bronze record_id
            StructField("bronze_ingestion_timestamp", TimestampType(), False),
            StructField("bronze_ingestion_date", DateType(), False),
            StructField("bronze_ingestion_hour", IntegerType(), False),

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

            # Silver Processing Metadata
            StructField("silver_processing_timestamp", TimestampType(), False),
            StructField("silver_processing_date", DateType(), False),  # Partition field
            StructField("silver_processing_hour", IntegerType(), False),  # Partition field

            # Payload information
            StructField("original_payload_size_bytes", IntegerType(), True),
            StructField("parsing_status", StringType(), False),  # SUCCESS, PARTIAL, FAILED
            StructField("parsing_errors", ArrayType(StringType()), True),

            # DQX Quality Metadata Fields - Added to SAME Silver Table
            StructField("flag_check", StringType(), False),  # PASS, FAIL, WARNING
            StructField("description_failure", StringType(), True),  # Details about validation failures
            StructField("dqx_rule_results", ArrayType(StringType()), True),  # DQX rule execution results
            StructField("dqx_quality_score", DoubleType(), False),  # Overall quality score (0.0-1.0)
            StructField("dqx_validation_timestamp", TimestampType(), False),  # When DQX validation occurred
            StructField("dqx_lineage_id", StringType(), True),  # DQX lineage tracking identifier
            StructField("dqx_criticality_level", StringType(), True),  # error, warn
            StructField("failed_rules_count", IntegerType(), False),  # Number of failed DQX rules
            StructField("passed_rules_count", IntegerType(), False)   # Number of passed DQX rules
        ])

    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            self.spark.sql(f"USE {self.config.database}")
            logger.info(f"✅ Database '{self.config.database}' ensured and selected")
        except Exception as e:
            logger.error(f"❌ Error creating database: {e}")
            raise

    def create_enhanced_silver_table(self) -> bool:
        """
        Create Enhanced Silver Delta table with DQX quality metadata.

        Returns:
            True if successful, False otherwise
        """
        try:
            table_exists = self._table_exists()
            table_path = self.get_table_path()

            if not table_exists:
                logger.info(f"📝 Creating Enhanced Silver table with DQX metadata: {self.full_table}")

                # Build CREATE TABLE DDL for Enhanced Silver with DQX fields
                ddl_columns = []
                for field in self.enhanced_silver_schema.fields:
                    spark_type = field.dataType.simpleString()
                    nullable = "" if field.nullable else "NOT NULL"
                    ddl_columns.append(f"`{field.name}` {spark_type} {nullable}")

                columns_ddl = ",\n  ".join(ddl_columns)

                create_table_sql = f"""
                CREATE TABLE {self.full_table} (
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
                  'description' = 'Enhanced Silver layer with DQX quality validation - Single Table',
                  'data_source' = 'Bronze layer with DQX quality framework',
                  'ingestion_pattern' = 'Streaming Bronze to Silver with DQX',
                  'data_format' = 'Purchase Order data with DQX quality metadata - ALL records',
                  'quality_framework' = 'Databricks DQX',
                  'storage_account' = '{self.config.storage_account}',
                  'container' = '{self.config.container}',
                  'dqx_enabled' = 'true',
                  'quality_criticality' = '{self.config.quality_criticality}',
                  'quality_threshold' = '{self.config.quality_threshold}',
                  'table_approach' = 'single_table_with_quality_flags',
                  'business_domain' = 'purchase_orders'
                )
                """

                self.spark.sql(create_table_sql)
                logger.info(f"✅ Enhanced Silver table created: {self.full_table}")
            else:
                logger.info(f"✅ Enhanced Silver table already exists: {self.full_table}")

            return True

        except Exception as e:
            logger.error(f"❌ Error creating Enhanced Silver table: {e}")
            return False

    def _table_exists(self) -> bool:
        """Check if table exists in Hive Metastore"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.full_table}")
            return True
        except:
            return False

    def get_table_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive table statistics including quality metrics.

        Returns:
            Dictionary with table statistics
        """
        try:
            stats = {}

            # Basic table information
            table_info = self.spark.sql(f"DESCRIBE DETAIL {self.full_table}").collect()[0]

            stats['table_info'] = {
                'location': table_info['location'],
                'format': table_info['format'],
                'numFiles': table_info['numFiles'],
                'sizeInBytes': table_info['sizeInBytes'],
                'createdAt': table_info['createdAt'],
                'lastModified': table_info['lastModified']
            }

            # Record counts
            total_records = self.spark.table(self.full_table).count()
            stats['record_counts'] = {'total': total_records}

            if total_records > 0:
                # Quality distribution
                quality_distribution = self.spark.table(self.full_table) \
                    .groupBy("flag_check") \
                    .count() \
                    .collect()

                quality_stats = {row['flag_check']: row['count'] for row in quality_distribution}
                stats['quality_distribution'] = quality_stats

                # Quality scores
                quality_metrics = self.spark.table(self.full_table) \
                    .select(
                        avg("dqx_quality_score").alias("avg_score"),
                        min("dqx_quality_score").alias("min_score"),
                        max("dqx_quality_score").alias("max_score")
                    ).collect()[0]

                stats['quality_metrics'] = {
                    'avg_score': quality_metrics['avg_score'],
                    'min_score': quality_metrics['min_score'],
                    'max_score': quality_metrics['max_score']
                }

                # Calculate quality rate
                pass_count = quality_stats.get('PASS', 0)
                quality_rate = (pass_count / total_records) if total_records > 0 else 0
                stats['quality_rate'] = quality_rate

            return stats

        except Exception as e:
            logger.error(f"❌ Error getting table statistics: {e}")
            return {'error': str(e)}

    def show_table_info(self):
        """Display comprehensive table information with quality metrics"""
        try:
            logger.info(f"📊 Enhanced Silver Table Information: {self.full_table}")
            logger.info("=" * 60)

            stats = self.get_table_statistics()

            if 'error' not in stats:
                # Table details
                table_info = stats['table_info']
                logger.info(f"📁 Location: {table_info['location']}")
                logger.info(f"📋 Format: {table_info['format']}")
                logger.info(f"📊 Number of Files: {table_info['numFiles']}")
                logger.info(f"💾 Size in Bytes: {table_info['sizeInBytes']:,}")
                logger.info(f"🕒 Created: {table_info['createdAt']}")
                logger.info(f"🔄 Last Modified: {table_info['lastModified']}")

                # Record counts and quality
                total_records = stats['record_counts']['total']
                logger.info(f"\n📊 Total Records: {total_records:,}")

                if 'quality_distribution' in stats:
                    logger.info("\n📊 Quality Status Distribution:")
                    for status, count in stats['quality_distribution'].items():
                        logger.info(f"   {status}: {count:,} records")

                    if 'quality_rate' in stats:
                        logger.info(f"\n📈 Quality Rate: {stats['quality_rate']:.2%}")

                        threshold_status = "✅ Above" if stats['quality_rate'] >= self.config.quality_threshold else "⚠️ Below"
                        logger.info(f"   vs Threshold: {threshold_status} {self.config.quality_threshold:.1%}")

                if 'quality_metrics' in stats:
                    metrics = stats['quality_metrics']
                    logger.info(f"\n📊 DQX Quality Scores:")
                    logger.info(f"   Average: {metrics['avg_score']:.3f}")
                    logger.info(f"   Range: {metrics['min_score']:.3f} - {metrics['max_score']:.3f}")
            else:
                logger.error(f"❌ Error retrieving table info: {stats['error']}")

        except Exception as e:
            logger.error(f"❌ Error displaying table info: {e}")

    def analyze_quality_failures(self, limit: int = 10):
        """
        Analyze quality failures in detail.

        Args:
            limit: Number of failure examples to show
        """
        try:
            logger.info("🚫 Quality Failures Analysis")
            logger.info("=" * 40)

            failed_count = self.spark.table(self.full_table) \
                .filter("flag_check = 'FAIL'") \
                .count()

            if failed_count > 0:
                logger.info(f"📊 Total Failed Records: {failed_count:,}")

                # Group by failure reasons
                logger.info("\n📋 Failure Reasons:")
                failure_reasons = self.spark.table(self.full_table) \
                    .filter("flag_check = 'FAIL'") \
                    .groupBy("description_failure") \
                    .count() \
                    .orderBy(col("count").desc()) \
                    .limit(limit) \
                    .collect()

                for row in failure_reasons:
                    logger.info(f"   {row['description_failure']}: {row['count']:,} records")

                # Show sample failed records
                logger.info(f"\n📋 Sample Failed Records (Top {limit}):")
                failed_samples = self.spark.table(self.full_table) \
                    .filter("flag_check = 'FAIL'") \
                    .select("order_id", "product_id", "customer_id", "description_failure",
                           "dqx_quality_score", "silver_processing_timestamp") \
                    .orderBy(col("silver_processing_timestamp").desc()) \
                    .limit(limit) \
                    .collect()

                for row in failed_samples:
                    logger.info(f"   Order: {row['order_id']}, Product: {row['product_id']}")
                    logger.info(f"     Failure: {row['description_failure']}")
                    logger.info(f"     Score: {row['dqx_quality_score']:.3f}")
                    logger.info("")

            else:
                logger.info("✅ No quality failures found - excellent data quality!")

        except Exception as e:
            logger.error(f"❌ Error analyzing quality failures: {e}")

    def get_quality_trends(self, days: int = 7) -> Dict[str, Any]:
        """
        Get quality trends over specified number of days.

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary with quality trends
        """
        try:
            trends = self.spark.table(self.full_table) \
                .filter(f"silver_processing_date >= date_sub(current_date(), {days})") \
                .groupBy("silver_processing_date") \
                .agg(
                    count("*").alias("total_records"),
                    sum(when(col("flag_check") == "PASS", 1).otherwise(0)).alias("pass_count"),
                    sum(when(col("flag_check") == "FAIL", 1).otherwise(0)).alias("fail_count"),
                    avg("dqx_quality_score").alias("avg_quality_score")
                ) \
                .withColumn("quality_rate", (col("pass_count") / col("total_records")) * 100) \
                .orderBy("silver_processing_date") \
                .collect()

            return {
                'daily_trends': [
                    {
                        'date': row['silver_processing_date'],
                        'total_records': row['total_records'],
                        'pass_count': row['pass_count'],
                        'fail_count': row['fail_count'],
                        'quality_rate': row['quality_rate'],
                        'avg_quality_score': row['avg_quality_score']
                    }
                    for row in trends
                ]
            }

        except Exception as e:
            logger.error(f"❌ Error getting quality trends: {e}")
            return {'error': str(e)}

    def optimize_table(self):
        """Optimize the Delta table for better performance"""
        try:
            logger.info(f"🔧 Optimizing table: {self.full_table}")

            # Run OPTIMIZE command
            self.spark.sql(f"OPTIMIZE {self.full_table}")

            # Run VACUUM to clean up old files (retain 7 days)
            self.spark.sql(f"VACUUM {self.full_table} RETAIN 168 HOURS")

            logger.info("✅ Table optimization completed")

        except Exception as e:
            logger.error(f"❌ Error optimizing table: {e}")
            raise

    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get detailed schema information.

        Returns:
            Dictionary with schema details
        """
        return {
            'table_name': self.full_table,
            'total_fields': len(self.enhanced_silver_schema.fields),
            'business_fields': len([f for f in self.enhanced_silver_schema.fields
                                  if f.name in ['order_id', 'product_id', 'customer_id', 'quantity', 'unit_price', 'total_amount']]),
            'technical_fields': len([f for f in self.enhanced_silver_schema.fields
                                   if f.name.startswith(('eventhub_', 'bronze_', 'silver_', 'processing_'))]),
            'dqx_fields': len([f for f in self.enhanced_silver_schema.fields
                             if f.name.startswith('dqx_') or f.name in ['flag_check', 'description_failure']]),
            'partition_fields': ['silver_processing_date', 'silver_processing_hour'],
            'quality_approach': 'single_table_with_flags'
        }