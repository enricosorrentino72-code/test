# DQX Utilities for Purchase Order Item Data Quality Validation

import logging
from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp
from decimal import Decimal

# DQX imports - these will be available in Databricks environment
try:
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
    from databricks.labs.dqx import check_funcs
    DQX_AVAILABLE = True
except ImportError:
    # For development/testing environment
    DQX_AVAILABLE = False
    logging.warning("DQX library not available - using mock implementation")

logger = logging.getLogger(__name__)

class DQXUtils:
    """
    Utility class for DQX (Data Quality eXtensions) operations specific to Purchase Order Items.

    Provides data quality rules, validation, and quality metrics for the pipeline.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize DQX utilities.

        Args:
            spark: Spark session
        """
        self.spark = spark
        self.engine = None

        if DQX_AVAILABLE:
            try:
                self.engine = DQEngine(spark)
                logger.info("DQX engine initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize DQX engine: {str(e)}")
                self.engine = None
        else:
            logger.warning("DQX not available - using mock implementation")

    def get_purchase_order_quality_rules(self) -> List[DQRowRule]:
        """
        Get comprehensive data quality rules for Purchase Order Items.

        Returns:
            List[DQRowRule]: List of DQX row-level quality rules
        """
        if not DQX_AVAILABLE or not self.engine:
            logger.warning("DQX not available - returning empty rules list")
            return []

        rules = []

        try:
            # Financial Calculation Validation Rules
            rules.append(
                DQRowRule(
                    name="total_amount_calculation_check",
                    description="Verify total_amount equals quantity * unit_price (with tolerance)",
                    rule_function=check_funcs.is_equal_to_expression,
                    rule_params={
                        "column": "total_amount",
                        "expression": "quantity * unit_price",
                        "tolerance": 0.01
                    },
                    critical=True
                )
            )

            # Business Logic Validation Rules
            rules.append(
                DQRowRule(
                    name="positive_quantity_check",
                    description="Quantity must be positive",
                    rule_function=check_funcs.is_greater_than,
                    rule_params={
                        "column": "quantity",
                        "value": 0
                    },
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="positive_unit_price_check",
                    description="Unit price must be positive",
                    rule_function=check_funcs.is_greater_than,
                    rule_params={
                        "column": "unit_price",
                        "value": 0.0
                    },
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="positive_total_amount_check",
                    description="Total amount must be positive",
                    rule_function=check_funcs.is_greater_than,
                    rule_params={
                        "column": "total_amount",
                        "value": 0.0
                    },
                    critical=True
                )
            )

            # Required Field Validation Rules
            rules.append(
                DQRowRule(
                    name="order_id_not_null_check",
                    description="Order ID cannot be null or empty",
                    rule_function=check_funcs.is_not_null,
                    rule_params={"column": "order_id"},
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="product_id_not_null_check",
                    description="Product ID cannot be null or empty",
                    rule_function=check_funcs.is_not_null,
                    rule_params={"column": "product_id"},
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="customer_id_not_null_check",
                    description="Customer ID cannot be null or empty",
                    rule_function=check_funcs.is_not_null,
                    rule_params={"column": "customer_id"},
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="vendor_id_not_null_check",
                    description="Vendor ID cannot be null or empty",
                    rule_function=check_funcs.is_not_null,
                    rule_params={"column": "vendor_id"},
                    critical=True
                )
            )

            # Format Validation Rules
            rules.append(
                DQRowRule(
                    name="order_id_format_check",
                    description="Order ID must follow ORD-XXXXXX format",
                    rule_function=check_funcs.matches_regex,
                    rule_params={
                        "column": "order_id",
                        "pattern": r"^ORD-\d{6}$"
                    },
                    critical=False
                )
            )

            rules.append(
                DQRowRule(
                    name="currency_code_check",
                    description="Currency must be valid 3-letter code",
                    rule_function=check_funcs.is_in_list,
                    rule_params={
                        "column": "currency",
                        "values": ["USD", "EUR", "GBP", "CAD", "AUD", "JPY"]
                    },
                    critical=False
                )
            )

            # Status Validation Rules
            rules.append(
                DQRowRule(
                    name="order_status_check",
                    description="Order status must be valid",
                    rule_function=check_funcs.is_in_list,
                    rule_params={
                        "column": "order_status",
                        "values": ["NEW", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
                    },
                    critical=True
                )
            )

            rules.append(
                DQRowRule(
                    name="payment_status_check",
                    description="Payment status must be valid",
                    rule_function=check_funcs.is_in_list,
                    rule_params={
                        "column": "payment_status",
                        "values": ["PENDING", "AUTHORIZED", "PAID", "FAILED", "REFUNDED"]
                    },
                    critical=True
                )
            )

            # Range Validation Rules
            rules.append(
                DQRowRule(
                    name="quantity_reasonable_range_check",
                    description="Quantity should be within reasonable range",
                    rule_function=check_funcs.is_between,
                    rule_params={
                        "column": "quantity",
                        "min_value": 1,
                        "max_value": 10000
                    },
                    critical=False
                )
            )

            rules.append(
                DQRowRule(
                    name="unit_price_reasonable_range_check",
                    description="Unit price should be within reasonable range",
                    rule_function=check_funcs.is_between,
                    rule_params={
                        "column": "unit_price",
                        "min_value": 0.01,
                        "max_value": 100000.00
                    },
                    critical=False
                )
            )

            # Timestamp Validation Rules
            rules.append(
                DQRowRule(
                    name="timestamp_not_future_check",
                    description="Timestamp should not be in the future",
                    rule_function=check_funcs.is_less_than_or_equal_to_current_timestamp,
                    rule_params={"column": "timestamp"},
                    critical=False
                )
            )

            rules.append(
                DQRowRule(
                    name="created_before_or_equal_timestamp_check",
                    description="Created_at should be before or equal to timestamp",
                    rule_function=check_funcs.is_less_than_or_equal_to_column,
                    rule_params={
                        "column": "created_at",
                        "other_column": "timestamp"
                    },
                    critical=False
                )
            )

            logger.info(f"Created {len(rules)} DQX quality rules for Purchase Order Items")

        except Exception as e:
            logger.error(f"Error creating DQX rules: {str(e)}")

        return rules

    def get_dataset_level_rules(self) -> List[DQDatasetRule]:
        """
        Get dataset-level quality rules for Purchase Order Items.

        Returns:
            List[DQDatasetRule]: List of dataset-level quality rules
        """
        if not DQX_AVAILABLE or not self.engine:
            logger.warning("DQX not available - returning empty dataset rules list")
            return []

        rules = []

        try:
            # Duplicate detection rules
            rules.append(
                DQDatasetRule(
                    name="no_duplicate_order_product_check",
                    description="No duplicate order_id + product_id combinations",
                    rule_function=check_funcs.has_no_duplicates,
                    rule_params={"columns": ["order_id", "product_id"]},
                    critical=True
                )
            )

            # Data freshness rules
            rules.append(
                DQDatasetRule(
                    name="data_freshness_check",
                    description="Data should not be older than 24 hours",
                    rule_function=check_funcs.has_recent_data,
                    rule_params={
                        "timestamp_column": "timestamp",
                        "max_age_hours": 24
                    },
                    critical=False
                )
            )

            # Volume validation rules
            rules.append(
                DQDatasetRule(
                    name="minimum_record_count_check",
                    description="Dataset should have minimum number of records",
                    rule_function=check_funcs.has_minimum_count,
                    rule_params={"min_count": 1},
                    critical=True
                )
            )

            logger.info(f"Created {len(rules)} dataset-level DQX rules")

        except Exception as e:
            logger.error(f"Error creating dataset rules: {str(e)}")

        return rules

    def apply_quality_checks(self,
                           df: DataFrame,
                           quality_threshold: float = 0.95) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Apply DQX quality checks to Purchase Order Items DataFrame.

        Args:
            df: Input DataFrame with purchase order items
            quality_threshold: Minimum quality threshold (0.0 to 1.0)

        Returns:
            Tuple[DataFrame, Dict]: Enhanced DataFrame with quality flags and quality metrics
        """
        if not DQX_AVAILABLE or not self.engine:
            logger.warning("DQX not available - using mock quality checks")
            return self._mock_quality_checks(df, quality_threshold)

        try:
            # Get quality rules
            row_rules = self.get_purchase_order_quality_rules()
            dataset_rules = self.get_dataset_level_rules()

            # Apply DQX checks
            result_df, invalid_df = self.engine.apply_checks_and_split(
                df=df,
                row_rules=row_rules,
                dataset_rules=dataset_rules
            )

            # Calculate quality metrics
            total_records = df.count()
            valid_records = result_df.count()
            invalid_records = invalid_df.count() if invalid_df else 0

            quality_score = valid_records / total_records if total_records > 0 else 0.0

            # Add DQX metadata to valid records
            result_df = result_df.withColumn("dqx_valid", lit(True)) \
                               .withColumn("dqx_timestamp", current_timestamp()) \
                               .withColumn("dqx_quality_score", lit(quality_score))

            # Add DQX metadata to invalid records
            if invalid_df:
                invalid_df = invalid_df.withColumn("dqx_valid", lit(False)) \
                                     .withColumn("dqx_timestamp", current_timestamp()) \
                                     .withColumn("dqx_quality_score", lit(quality_score))

                # Combine valid and invalid records for single table approach
                combined_df = result_df.unionByName(invalid_df)
            else:
                combined_df = result_df

            # Quality metrics
            quality_metrics = {
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": invalid_records,
                "quality_score": quality_score,
                "quality_threshold": quality_threshold,
                "quality_passed": quality_score >= quality_threshold,
                "rules_applied": len(row_rules) + len(dataset_rules),
                "processing_timestamp": current_timestamp()
            }

            logger.info(f"DQX quality checks completed. Score: {quality_score:.4f}, "
                       f"Valid: {valid_records}, Invalid: {invalid_records}")

            return combined_df, quality_metrics

        except Exception as e:
            logger.error(f"Error applying DQX quality checks: {str(e)}")
            # Fallback to mock implementation
            return self._mock_quality_checks(df, quality_threshold)

    def _mock_quality_checks(self,
                           df: DataFrame,
                           quality_threshold: float = 0.95) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Mock quality checks for testing/development when DQX is not available.

        Args:
            df: Input DataFrame
            quality_threshold: Quality threshold

        Returns:
            Tuple[DataFrame, Dict]: DataFrame with mock quality flags and metrics
        """
        try:
            # Simple mock validation using Spark SQL
            result_df = df.withColumn(
                "dqx_valid",
                when((col("quantity") > 0) &
                     (col("unit_price") > 0) &
                     (col("total_amount") > 0) &
                     (col("order_id").isNotNull()) &
                     (col("product_id").isNotNull()) &
                     (col("customer_id").isNotNull()) &
                     (col("vendor_id").isNotNull()), True
                ).otherwise(False)
            ).withColumn("dqx_timestamp", current_timestamp()) \
             .withColumn("dqx_check_results", lit("mock_validation"))

            # Calculate mock quality score
            total_records = df.count()
            valid_records = result_df.filter(col("dqx_valid") == True).count()
            quality_score = valid_records / total_records if total_records > 0 else 0.0

            result_df = result_df.withColumn("dqx_quality_score", lit(quality_score))

            quality_metrics = {
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": total_records - valid_records,
                "quality_score": quality_score,
                "quality_threshold": quality_threshold,
                "quality_passed": quality_score >= quality_threshold,
                "rules_applied": 8,  # Mock rule count
                "processing_timestamp": current_timestamp(),
                "mock_implementation": True
            }

            logger.info(f"Mock quality checks completed. Score: {quality_score:.4f}")

            return result_df, quality_metrics

        except Exception as e:
            logger.error(f"Error in mock quality checks: {str(e)}")
            raise

    def generate_quality_report(self, quality_metrics: Dict[str, Any]) -> str:
        """
        Generate a quality report from quality metrics.

        Args:
            quality_metrics: Quality metrics dictionary

        Returns:
            str: Formatted quality report
        """
        try:
            report = f"""
=== Purchase Order Item Data Quality Report ===

Processing Timestamp: {quality_metrics.get('processing_timestamp', 'N/A')}

Record Summary:
  Total Records: {quality_metrics.get('total_records', 0):,}
  Valid Records: {quality_metrics.get('valid_records', 0):,}
  Invalid Records: {quality_metrics.get('invalid_records', 0):,}

Quality Score: {quality_metrics.get('quality_score', 0.0):.4f} ({quality_metrics.get('quality_score', 0.0)*100:.2f}%)
Quality Threshold: {quality_metrics.get('quality_threshold', 0.0):.4f} ({quality_metrics.get('quality_threshold', 0.0)*100:.2f}%)

Quality Assessment: {'PASSED' if quality_metrics.get('quality_passed', False) else 'FAILED'}

Rules Applied: {quality_metrics.get('rules_applied', 0)}
Implementation: {'Mock' if quality_metrics.get('mock_implementation', False) else 'DQX'}

===============================================
            """

            return report.strip()

        except Exception as e:
            logger.error(f"Error generating quality report: {str(e)}")
            return f"Error generating quality report: {str(e)}"

    def get_quality_summary_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get summary quality metrics from a DataFrame with DQX results.

        Args:
            df: DataFrame with DQX quality results

        Returns:
            Dict[str, Any]: Summary quality metrics
        """
        try:
            # Check if DQX columns exist
            dqx_columns = ["dqx_valid", "dqx_quality_score", "dqx_timestamp"]
            has_dqx_columns = all(col_name in df.columns for col_name in dqx_columns)

            if not has_dqx_columns:
                logger.warning("DataFrame does not contain DQX result columns")
                return {"error": "No DQX results found in DataFrame"}

            total_count = df.count()
            valid_count = df.filter(col("dqx_valid") == True).count()
            invalid_count = total_count - valid_count

            # Get quality score (should be same for all rows)
            quality_score = df.select("dqx_quality_score").first()[0] if total_count > 0 else 0.0

            # Get latest processing timestamp
            latest_timestamp = df.select("dqx_timestamp").orderBy(col("dqx_timestamp").desc()).first()[0]

            summary = {
                "total_records": total_count,
                "valid_records": valid_count,
                "invalid_records": invalid_count,
                "quality_score": float(quality_score),
                "quality_percentage": float(quality_score) * 100,
                "latest_processing_timestamp": latest_timestamp,
                "has_quality_issues": invalid_count > 0
            }

            return summary

        except Exception as e:
            logger.error(f"Error getting quality summary: {str(e)}")
            return {"error": str(e)}

    @staticmethod
    def create_quality_dashboard_data(quality_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create data structure for quality monitoring dashboard.

        Args:
            quality_metrics: Quality metrics from DQX processing

        Returns:
            Dict[str, Any]: Dashboard-ready quality data
        """
        try:
            dashboard_data = {
                "timestamp": quality_metrics.get("processing_timestamp"),
                "pipeline": "purchase_order_item",
                "quality_score": quality_metrics.get("quality_score", 0.0),
                "quality_percentage": quality_metrics.get("quality_score", 0.0) * 100,
                "total_records": quality_metrics.get("total_records", 0),
                "valid_records": quality_metrics.get("valid_records", 0),
                "invalid_records": quality_metrics.get("invalid_records", 0),
                "quality_passed": quality_metrics.get("quality_passed", False),
                "threshold": quality_metrics.get("quality_threshold", 0.0),
                "rules_count": quality_metrics.get("rules_applied", 0),
                "status": "PASSED" if quality_metrics.get("quality_passed", False) else "FAILED"
            }

            return dashboard_data

        except Exception as e:
            logger.error(f"Error creating dashboard data: {str(e)}")
            return {"error": str(e)}