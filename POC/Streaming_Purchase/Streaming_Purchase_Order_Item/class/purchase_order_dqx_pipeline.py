"""
Purchase Order DQX Pipeline Module

This module implements the Bronze to Silver streaming pipeline with DQX quality validation
for purchase order items. It follows a single enhanced table approach where all records
(valid and invalid) are stored in the same table with quality flags.

Author: DataOps Team
Date: 2024
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the DQX pipeline"""
    bronze_database: str
    bronze_table: str
    silver_database: str
    silver_table: str
    silver_path: str
    checkpoint_path: str
    storage_account: str
    container: str
    quality_criticality: str = "error"
    quality_threshold: float = 0.95
    trigger_mode: str = "5 seconds"
    max_events_per_trigger: int = 10000


class PurchaseOrderDQXPipeline:
    """
    Bronze to Silver streaming pipeline with DQX quality validation for purchase orders.

    This class implements a structured streaming pipeline that reads from Bronze layer,
    applies DQX quality validation, and writes to an enhanced Silver layer with quality metadata.
    Uses single table approach where all records contain quality flags.
    """

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        """
        Initialize the DQX pipeline.

        Args:
            spark: Active Spark session
            config: Pipeline configuration
        """
        self.spark = spark
        self.config = config
        self.streaming_query = None

        # Full table names
        self.bronze_full_table = f"{config.bronze_database}.{config.bronze_table}"
        self.silver_full_table = f"{config.silver_database}.{config.silver_table}"

        # Initialize DQX components
        self._initialize_dqx_components()

        # Configure Spark for streaming
        self._configure_spark_streaming()

        logger.info(f"✅ PurchaseOrderDQXPipeline initialized")
        logger.info(f"   Source: {self.bronze_full_table}")
        logger.info(f"   Target: {self.silver_full_table}")

    def _initialize_dqx_components(self):
        """Initialize DQX framework components"""

        self.dqx_available = False
        self.dq_engine = None
        self.quality_rules = []

        try:
            from databricks.labs.dqx.engine import DQEngine
            from databricks.sdk import WorkspaceClient

            # Initialize DQX engine
            try:
                ws = WorkspaceClient()
                self.dq_engine = DQEngine(ws)
            except:
                self.dq_engine = DQEngine()

            self.dqx_available = True
            logger.info("✅ DQX Framework initialized successfully")

        except ImportError as e:
            logger.warning(f"⚠️ DQX Framework not available: {e}")
            logger.info("🔄 Will use basic validation fallback")

    def _configure_spark_streaming(self):
        """Configure Spark session for optimal streaming performance"""

        spark_configs = {
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.databricks.delta.autoOptimize.optimizeWrite": "true",
            "spark.databricks.delta.autoOptimize.autoCompact": "true",
            "spark.sql.streaming.checkpointLocation.compression.codec": "lz4"
        }

        for key, value in spark_configs.items():
            self.spark.conf.set(key, value)

        logger.info("✅ Spark configured for DQX-enhanced streaming pipeline")

    def parse_purchase_order_payload(self, df: DataFrame) -> DataFrame:
        """
        Parse JSON payload and extract purchase order data.

        Args:
            df: Bronze layer DataFrame with raw JSON payload

        Returns:
            DataFrame with parsed purchase order fields
        """
        logger.info("📊 Parsing purchase order JSON payload...")

        # Parse JSON fields from raw_payload
        parsed_df = df.select(
            # Inherited fields from Bronze
            col("record_id").alias("source_record_id"),
            col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            col("ingestion_date").alias("bronze_ingestion_date"),
            col("ingestion_hour").alias("bronze_ingestion_hour"),

            # Event Hub technical fields
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_sequence_number"),
            col("eventhub_enqueued_time"),
            col("eventhub_partition_key"),

            # Processing context
            col("processing_cluster_id"),
            col("consumer_group"),
            col("eventhub_name"),

            # Original payload info
            col("payload_size_bytes").alias("original_payload_size_bytes"),
            col("raw_payload"),

            # Parse PurchaseOrderItem fields from JSON
            get_json_object(col("raw_payload"), "$.order_id").alias("order_id"),
            get_json_object(col("raw_payload"), "$.product_id").alias("product_id"),
            get_json_object(col("raw_payload"), "$.customer_id").alias("customer_id"),
            get_json_object(col("raw_payload"), "$.product_name").alias("product_name"),
            get_json_object(col("raw_payload"), "$.quantity").cast("integer").alias("quantity"),
            get_json_object(col("raw_payload"), "$.unit_price").cast("decimal(10,2)").alias("unit_price"),
            get_json_object(col("raw_payload"), "$.total_amount").cast("decimal(10,2)").alias("total_amount"),
            get_json_object(col("raw_payload"), "$.discount_percentage").cast("decimal(5,2)").alias("discount_percentage"),
            get_json_object(col("raw_payload"), "$.tax_amount").cast("decimal(10,2)").alias("tax_amount"),
            get_json_object(col("raw_payload"), "$.order_status").alias("order_status"),
            get_json_object(col("raw_payload"), "$.payment_status").alias("payment_status"),
            get_json_object(col("raw_payload"), "$.order_date").cast("timestamp").alias("order_date"),
            get_json_object(col("raw_payload"), "$.ship_date").cast("timestamp").alias("ship_date"),
            get_json_object(col("raw_payload"), "$.delivery_date").cast("timestamp").alias("delivery_date"),
            get_json_object(col("raw_payload"), "$.customer_email").alias("customer_email"),
            get_json_object(col("raw_payload"), "$.shipping_address").alias("shipping_address"),
            get_json_object(col("raw_payload"), "$.billing_address").alias("billing_address"),
            get_json_object(col("raw_payload"), "$.product_category").alias("product_category"),
            get_json_object(col("raw_payload"), "$.warehouse_id").alias("warehouse_id"),
            get_json_object(col("raw_payload"), "$.shipping_method").alias("shipping_method"),
            get_json_object(col("raw_payload"), "$.tracking_number").alias("tracking_number"),
            get_json_object(col("raw_payload"), "$.notes").alias("notes"),

            # Additional metadata fields
            get_json_object(col("raw_payload"), "$.cluster_id").alias("purchase_cluster_id"),
            get_json_object(col("raw_payload"), "$.notebook_path").alias("notebook_path")
        )

        # Add parsing status based on critical fields
        parsed_df = parsed_df.withColumn(
            "parsing_status",
            when(
                col("order_id").isNotNull() &
                col("product_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("quantity").isNotNull() &
                col("unit_price").isNotNull(),
                lit("SUCCESS")
            ).when(
                col("order_id").isNotNull() |
                col("product_id").isNotNull() |
                col("customer_id").isNotNull(),
                lit("PARTIAL")
            ).otherwise(lit("FAILED"))
        )

        # Add parsing errors array
        parsed_df = parsed_df.withColumn(
            "parsing_errors",
            when(col("parsing_status") == "FAILED",
                 array(lit("JSON parsing completely failed"))
            ).when(col("parsing_status") == "PARTIAL",
                   array(lit("Some required fields missing from JSON"))
            ).otherwise(array())  # Empty array for SUCCESS
        )

        # Handle missing values with defaults
        parsed_df = self._handle_missing_values(parsed_df)

        return parsed_df

    def _handle_missing_values(self, df: DataFrame) -> DataFrame:
        """
        Handle missing values with appropriate defaults.

        Args:
            df: DataFrame with parsed data

        Returns:
            DataFrame with handled missing values
        """
        # Generate order_id if missing
        df = df.withColumn(
            "order_id",
            coalesce(
                col("order_id"),
                concat(
                    lit("ORD-"),
                    unix_timestamp(col("bronze_ingestion_timestamp")).cast("string")
                )
            )
        )

        # Use bronze ingestion timestamp if order_date is missing
        df = df.withColumn(
            "order_date",
            coalesce(col("order_date"), col("bronze_ingestion_timestamp"))
        )

        # Set default statuses
        df = df.withColumn(
            "order_status",
            coalesce(col("order_status"), lit("NEW"))
        ).withColumn(
            "payment_status",
            coalesce(col("payment_status"), lit("PENDING"))
        )

        # Set numeric defaults
        df = df.fillna({
            "quantity": 0,
            "unit_price": 0.0,
            "total_amount": 0.0,
            "discount_percentage": 0.0,
            "tax_amount": 0.0
        })

        return df

    def apply_dqx_quality_validation_single_table(self, df: DataFrame, rules: List) -> DataFrame:
        """
        Apply DQX quality validation and add quality metadata to ALL records.

        This implements the single table approach where all records (valid and invalid)
        are stored in the same table with quality flags.

        Args:
            df: DataFrame to validate
            rules: List of DQX quality rules

        Returns:
            DataFrame with quality metadata added to all records
        """
        logger.info("🔍 Applying DQX quality validation - single table approach...")

        if not self.dqx_available or not rules:
            logger.warning("⚠️ DQX not available - using basic validation fallback")
            return self._apply_basic_quality_validation_single_table(df)

        try:
            # Apply DQX validation
            if self.dq_engine and hasattr(self.dq_engine, 'apply_checks_and_split'):
                # DQX typically splits data, but we'll rejoin for single table
                valid_data, invalid_data = self.dq_engine.apply_checks_and_split(df, rules)

                # Add quality metadata to valid records
                valid_with_metadata = self._add_dqx_quality_metadata(valid_data, is_valid=True)

                # Add quality metadata to invalid records
                invalid_with_metadata = self._add_dqx_quality_metadata(invalid_data, is_valid=False)

                # Single Table Approach: Union all records back together
                all_records_with_quality = valid_with_metadata.unionByName(invalid_with_metadata)

                logger.info("✅ DQX quality validation completed - all records annotated")
                return all_records_with_quality
            else:
                logger.warning("⚠️ DQX apply_checks_and_split not available")
                return self._apply_basic_quality_validation_single_table(df)

        except Exception as e:
            logger.error(f"❌ DQX quality validation failed: {e}")
            return self._apply_basic_quality_validation_single_table(df)

    def _apply_basic_quality_validation_single_table(self, df: DataFrame) -> DataFrame:
        """
        Fallback basic quality validation when DQX is not available.

        Args:
            df: DataFrame to validate

        Returns:
            DataFrame with basic quality metadata
        """
        logger.info("🔄 Applying basic quality validation (DQX fallback)...")

        # Define basic validation conditions
        quality_conditions = [
            col("order_id").isNotNull(),
            col("product_id").isNotNull(),
            col("customer_id").isNotNull(),
            col("quantity") > 0,
            col("unit_price") > 0,
            # Financial validation: total_amount should equal quantity * unit_price
            abs(col("total_amount") - (col("quantity") * col("unit_price"))) <= 0.01,
            col("parsing_status").isin(["SUCCESS", "PARTIAL"])
        ]

        # Combine all conditions
        quality_condition = reduce(lambda a, b: a & b, quality_conditions)

        # Add quality metadata to ALL records based on conditions
        df_with_quality = df.withColumn(
            "flag_check",
            when(quality_condition, lit("PASS")).otherwise(lit("FAIL"))
        ).withColumn(
            "description_failure",
            when(
                quality_condition,
                lit(None).cast("string")
            ).otherwise(
                concat(
                    lit("Validation failed: "),
                    when(col("order_id").isNull(), lit("order_id missing, ")).otherwise(lit("")),
                    when(col("quantity") <= 0, lit("invalid quantity, ")).otherwise(lit("")),
                    when(col("unit_price") <= 0, lit("invalid unit_price, ")).otherwise(lit("")),
                    when(
                        abs(col("total_amount") - (col("quantity") * col("unit_price"))) > 0.01,
                        lit("total_amount calculation mismatch, ")
                    ).otherwise(lit("")),
                    when(col("parsing_status") == "FAILED", lit("JSON parsing failed")).otherwise(lit(""))
                )
            )
        ).withColumn(
            "dqx_rule_results",
            when(
                quality_condition,
                array(lit("Basic validation passed"))
            ).otherwise(
                array(lit("Basic validation failed"))
            )
        ).withColumn(
            "dqx_quality_score",
            when(quality_condition, lit(0.8)).otherwise(lit(0.0))
        ).withColumn(
            "dqx_validation_timestamp", current_timestamp()
        ).withColumn(
            "dqx_lineage_id",
            concat(lit("basic-"), lit(str(uuid.uuid4())))
        ).withColumn(
            "dqx_criticality_level", lit(self.config.quality_criticality)
        ).withColumn(
            "failed_rules_count",
            when(quality_condition, lit(0)).otherwise(lit(1))
        ).withColumn(
            "passed_rules_count",
            when(quality_condition, lit(1)).otherwise(lit(0))
        )

        return df_with_quality

    def _add_dqx_quality_metadata(self, df: DataFrame, is_valid: bool) -> DataFrame:
        """
        Add DQX-specific quality metadata fields to records.

        Args:
            df: DataFrame to add metadata to
            is_valid: Whether the records passed validation

        Returns:
            DataFrame with DQX quality metadata
        """
        # Generate unique DQX lineage ID
        dqx_lineage_id = f"dqx-{uuid.uuid4()}"

        if is_valid:
            return df.withColumn("flag_check", lit("PASS")) \
                    .withColumn("description_failure", lit(None).cast("string")) \
                    .withColumn("dqx_rule_results", array(lit("All DQX rules passed"))) \
                    .withColumn("dqx_quality_score", lit(1.0)) \
                    .withColumn("dqx_validation_timestamp", current_timestamp()) \
                    .withColumn("dqx_lineage_id", lit(dqx_lineage_id)) \
                    .withColumn("dqx_criticality_level", lit(self.config.quality_criticality)) \
                    .withColumn("failed_rules_count", lit(0)) \
                    .withColumn("passed_rules_count", lit(len(self.quality_rules)))
        else:
            return df.withColumn("flag_check", lit("FAIL")) \
                    .withColumn("description_failure", lit("DQX validation failed - one or more quality rules not met")) \
                    .withColumn("dqx_rule_results", array(lit("DQX validation failed"))) \
                    .withColumn("dqx_quality_score", lit(0.0)) \
                    .withColumn("dqx_validation_timestamp", current_timestamp()) \
                    .withColumn("dqx_lineage_id", lit(dqx_lineage_id)) \
                    .withColumn("dqx_criticality_level", lit(self.config.quality_criticality)) \
                    .withColumn("failed_rules_count", lit(len(self.quality_rules))) \
                    .withColumn("passed_rules_count", lit(0))

    def add_silver_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add Silver layer processing metadata.

        Args:
            df: DataFrame to add metadata to

        Returns:
            DataFrame with Silver processing metadata
        """
        return df.withColumn(
            "silver_processing_timestamp", current_timestamp()
        ).withColumn(
            "silver_processing_date", current_date()
        ).withColumn(
            "silver_processing_hour", hour(current_timestamp())
        )

    def transform_bronze_to_silver_dqx(self, bronze_df: DataFrame, quality_rules: List) -> DataFrame:
        """
        Complete transformation from Bronze to Silver with DQX quality validation.

        Args:
            bronze_df: Bronze layer DataFrame
            quality_rules: List of DQX quality rules

        Returns:
            Enhanced Silver DataFrame with quality metadata
        """
        logger.info("🔄 Starting Bronze to Silver transformation with DQX validation...")

        # Step 1: Parse purchase order data from JSON payload
        parsed_df = self.parse_purchase_order_payload(bronze_df)

        # Step 2: Apply DQX quality validation and add quality metadata to ALL records
        df_with_quality = self.apply_dqx_quality_validation_single_table(parsed_df, quality_rules)

        # Step 3: Add Silver processing metadata
        silver_df = self.add_silver_processing_metadata(df_with_quality)

        logger.info("✅ Bronze to Silver DQX transformation completed")
        return silver_df

    def get_trigger_config(self) -> Dict[str, str]:
        """
        Get trigger configuration for streaming.

        Returns:
            Dictionary with trigger configuration
        """
        trigger_mode = self.config.trigger_mode

        if "second" in trigger_mode:
            interval = trigger_mode.split()[0]
            return {"processingTime": f"{interval} seconds"}
        elif "minute" in trigger_mode:
            interval = trigger_mode.split()[0]
            return {"processingTime": f"{interval} minutes"}
        else:
            return {"processingTime": "5 seconds"}

    def start_streaming(self, quality_rules: List) -> StreamingQuery:
        """
        Start the Bronze to Silver streaming pipeline with DQX validation.

        Args:
            quality_rules: List of DQX quality rules to apply

        Returns:
            StreamingQuery object
        """
        logger.info("🚀 Starting Bronze to Silver DQX Streaming Pipeline")
        logger.info("=" * 60)
        logger.info(f"📊 Source: {self.bronze_full_table}")
        logger.info(f"✨ Target: {self.silver_full_table}")
        logger.info(f"📋 Approach: Single table with quality flags")
        logger.info(f"🔍 Quality Framework: {'Databricks DQX' if self.dqx_available else 'Basic Validation'}")
        logger.info(f"📏 Quality Threshold: {self.config.quality_threshold}")
        logger.info(f"⚡ Trigger: {self.config.trigger_mode}")
        logger.info(f"📦 Max Events/Trigger: {self.config.max_events_per_trigger}")
        logger.info(f"🔄 Checkpoint: {self.config.checkpoint_path}")
        logger.info("=" * 60)

        try:
            # Store quality rules for use in transformations
            self.quality_rules = quality_rules

            # Read from Bronze table as stream
            logger.info("📖 Reading Bronze table stream...")
            bronze_stream = self.spark \
                .readStream \
                .format("delta") \
                .table(self.bronze_full_table)

            logger.info("✅ Bronze stream source created")

            # Apply transformation to Enhanced Silver with DQX
            logger.info("🔄 Applying Bronze to Silver transformation with DQX...")
            silver_stream = self.transform_bronze_to_silver_dqx(bronze_stream, quality_rules)

            logger.info("✅ Enhanced Silver stream transformation configured")

            # Configure trigger
            trigger_config = self.get_trigger_config()
            logger.info(f"✅ Trigger configured: {trigger_config}")

            # Write to Enhanced Silver table with checkpointing
            self.streaming_query = silver_stream \
                .writeStream \
                .outputMode("append") \
                .format("delta") \
                .option("checkpointLocation", self.config.checkpoint_path) \
                .option("maxRecordsPerFile", "50000") \
                .trigger(**trigger_config) \
                .toTable(self.silver_full_table)

            logger.info("✅ Bronze to Silver DQX pipeline started successfully")
            logger.info(f"📊 Query ID: {self.streaming_query.id}")
            logger.info(f"🔄 Run ID: {self.streaming_query.runId}")
            logger.info(f"📝 Mode: APPEND-ONLY")
            logger.info(f"🗄️ Writing to Enhanced Silver table: {self.silver_full_table}")

            return self.streaming_query

        except Exception as e:
            logger.error(f"❌ Failed to start DQX pipeline: {e}")
            raise

    def stop_streaming(self):
        """Stop the streaming pipeline gracefully"""

        if self.streaming_query and self.streaming_query.isActive:
            logger.info("⏹️ Stopping DQX streaming pipeline...")
            self.streaming_query.stop()

            # Wait for graceful shutdown
            import time
            timeout = 30
            elapsed = 0
            while self.streaming_query.isActive and elapsed < timeout:
                time.sleep(1)
                elapsed += 1

            if not self.streaming_query.isActive:
                logger.info("✅ DQX pipeline stopped successfully")
            else:
                logger.warning("⚠️ Pipeline stop timeout")
        else:
            logger.info("ℹ️ Pipeline is already stopped")

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.

        Returns:
            Dictionary with pipeline status information
        """
        if self.streaming_query:
            return {
                'query_id': self.streaming_query.id,
                'run_id': self.streaming_query.runId,
                'is_active': self.streaming_query.isActive,
                'bronze_table': self.bronze_full_table,
                'silver_table': self.silver_full_table,
                'dqx_available': self.dqx_available,
                'quality_threshold': self.config.quality_threshold,
                'trigger_mode': self.config.trigger_mode
            }
        else:
            return {
                'status': 'Pipeline not started',
                'bronze_table': self.bronze_full_table,
                'silver_table': self.silver_full_table
            }