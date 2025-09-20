"""
Purchase Order DQX Monitor Module

This module provides comprehensive monitoring and dashboard capabilities for the
Purchase Order DQX pipeline. It tracks quality metrics, streaming performance,
and provides real-time insights into data quality trends.

Author: DataOps Team
Date: 2024
"""

import logging
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class MonitorConfig:
    """Configuration for monitoring settings"""
    refresh_interval: int = 30  # seconds
    dashboard_auto_refresh: bool = True
    alert_threshold: float = 0.8  # Quality rate below this triggers alert
    max_failure_samples: int = 10


class PurchaseOrderDQXMonitor:
    """
    Monitor DQX pipeline and provide comprehensive quality metrics dashboard.

    This class provides real-time monitoring of the Purchase Order DQX pipeline,
    including streaming performance, quality metrics, and failure analysis.
    """

    def __init__(self, spark: SparkSession, silver_table: str, config: Optional[MonitorConfig] = None):
        """
        Initialize the DQX monitor.

        Args:
            spark: Active Spark session
            silver_table: Full name of the Silver table to monitor
            config: Optional monitoring configuration
        """
        self.spark = spark
        self.silver_table = silver_table
        self.config = config or MonitorConfig()

        logger.info(f"✅ PurchaseOrderDQXMonitor initialized")
        logger.info(f"   Monitoring table: {silver_table}")

    def display_quality_dashboard(self, streaming_query: Optional[StreamingQuery] = None):
        """
        Display comprehensive quality dashboard with real-time metrics.

        Args:
            streaming_query: Optional streaming query for pipeline status
        """
        try:
            # Get current metrics
            metrics = self._get_current_metrics()
            streaming_status = self._get_streaming_status(streaming_query)

            # Generate dashboard HTML
            dashboard_html = self._generate_dashboard_html(metrics, streaming_status)

            # Display dashboard (this would work in Databricks environment)
            try:
                displayHTML(dashboard_html)
            except NameError:
                # Fallback to console output if not in Databricks
                self._display_console_dashboard(metrics, streaming_status)

        except Exception as e:
            logger.error(f"⚠️ Dashboard display error: {e}")
            self._display_console_dashboard_fallback()

    def _get_current_metrics(self) -> Dict[str, Any]:
        """Get current quality and performance metrics"""

        try:
            # Check if table exists and has data
            total_records = self.spark.table(self.silver_table).count()

            if total_records == 0:
                return {
                    'total_records': 0,
                    'quality_distribution': {},
                    'quality_rate': 0.0,
                    'avg_quality_score': 0.0,
                    'recent_records': 0,
                    'has_data': False
                }

            # Quality distribution
            quality_distribution = self.spark.table(self.silver_table) \
                .groupBy("flag_check") \
                .count() \
                .collect()

            quality_stats = {row['flag_check']: row['count'] for row in quality_distribution}
            pass_count = quality_stats.get('PASS', 0)
            fail_count = quality_stats.get('FAIL', 0)
            warning_count = quality_stats.get('WARNING', 0)

            # Quality rate
            quality_rate = (pass_count / total_records) if total_records > 0 else 0

            # Average quality score
            avg_score_result = self.spark.table(self.silver_table) \
                .select(avg("dqx_quality_score").alias("avg_score")).collect()[0]
            avg_quality_score = avg_score_result["avg_score"] if avg_score_result["avg_score"] else 0.0

            # Recent records (today)
            recent_records = self.spark.table(self.silver_table) \
                .filter("silver_processing_date >= current_date()") \
                .count()

            # Failed rules analysis
            failed_rules_analysis = self._analyze_failed_rules()

            return {
                'total_records': total_records,
                'quality_distribution': {
                    'PASS': pass_count,
                    'FAIL': fail_count,
                    'WARNING': warning_count
                },
                'quality_rate': quality_rate,
                'avg_quality_score': avg_quality_score,
                'recent_records': recent_records,
                'failed_rules_analysis': failed_rules_analysis,
                'has_data': True
            }

        except Exception as e:
            logger.error(f"❌ Error getting current metrics: {e}")
            return {'error': str(e), 'has_data': False}

    def _get_streaming_status(self, streaming_query: Optional[StreamingQuery]) -> Dict[str, Any]:
        """Get streaming pipeline status information"""

        if streaming_query:
            try:
                progress = streaming_query.lastProgress
                return {
                    'is_active': streaming_query.isActive,
                    'query_id': streaming_query.id,
                    'run_id': streaming_query.runId,
                    'batch_id': progress.get('batchId', 0) if progress else 0,
                    'input_rows_per_second': progress.get('inputRowsPerSecond', 0) if progress else 0,
                    'processed_rows_per_second': progress.get('processedRowsPerSecond', 0) if progress else 0,
                    'batch_duration_ms': progress.get('batchDuration', 0) if progress else 0
                }
            except Exception as e:
                logger.warning(f"⚠️ Error getting streaming status: {e}")
                return {'is_active': False, 'error': str(e)}
        else:
            return {'is_active': False, 'status': 'No streaming query provided'}

    def _analyze_failed_rules(self) -> Dict[str, Any]:
        """Analyze which rules are failing most frequently"""

        try:
            failed_records = self.spark.table(self.silver_table) \
                .filter("flag_check = 'FAIL'") \
                .count()

            if failed_records == 0:
                return {'total_failures': 0, 'top_failure_reasons': []}

            # Get top failure reasons
            failure_reasons = self.spark.table(self.silver_table) \
                .filter("flag_check = 'FAIL'") \
                .groupBy("description_failure") \
                .count() \
                .orderBy(col("count").desc()) \
                .limit(5) \
                .collect()

            top_reasons = [
                {'reason': row['description_failure'], 'count': row['count']}
                for row in failure_reasons
            ]

            return {
                'total_failures': failed_records,
                'top_failure_reasons': top_reasons
            }

        except Exception as e:
            logger.error(f"❌ Error analyzing failed rules: {e}")
            return {'error': str(e)}

    def _generate_dashboard_html(self, metrics: Dict[str, Any], streaming_status: Dict[str, Any]) -> str:
        """Generate HTML dashboard for Databricks display"""

        if not metrics.get('has_data', False):
            return self._generate_no_data_html()

        total_records = metrics['total_records']
        quality_dist = metrics['quality_distribution']
        quality_rate = metrics['quality_rate']
        avg_score = metrics['avg_quality_score']
        recent_records = metrics['recent_records']

        # Determine status colors
        quality_status_color = "#4CAF50" if quality_rate >= 0.9 else "#FF9800" if quality_rate >= 0.7 else "#F44336"
        streaming_status_color = "#4CAF50" if streaming_status.get('is_active', False) else "#F44336"

        dashboard_html = f"""
        <div style="border: 2px solid #4CAF50; border-radius: 15px; padding: 20px; background: #f8f9fa; font-family: Arial, sans-serif;">
            <h2 style="color: #4CAF50; text-align: center; margin-bottom: 20px;">
                🔍 Purchase Order DQX Pipeline Dashboard
            </h2>

            <div style="background: #e8f5e8; padding: 15px; border-radius: 10px; border-left: 5px solid #4CAF50; margin: 15px 0;">
                <h4 style="margin: 0; color: #2e7d2e;">📊 Single Enhanced Table Approach</h4>
                <p style="margin: 5px 0; color: #2e7d2e;">
                    <strong>ALL records</strong> stored in same table: <code>{self.silver_table}</code>
                </p>
                <p style="margin: 5px 0; color: #2e7d2e;">
                    Quality tracking via <strong>flag_check</strong> field (PASS/FAIL/WARNING)
                </p>
            </div>

            <!-- Main Metrics Grid -->
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0;">
                <div style="text-align: center; padding: 15px; background: white; border-radius: 10px; border-left: 5px solid #4CAF50;">
                    <h4 style="margin: 0; color: #4CAF50;">Total Records</h4>
                    <p style="font-size: 28px; margin: 5px 0; font-weight: bold;">{total_records:,}</p>
                    <p style="margin: 0; color: #666;">Enhanced Silver</p>
                </div>

                <div style="text-align: center; padding: 15px; background: white; border-radius: 10px; border-left: 5px solid #2196F3;">
                    <h4 style="margin: 0; color: #2196F3;">DQX PASS</h4>
                    <p style="font-size: 28px; margin: 5px 0; font-weight: bold;">{quality_dist.get('PASS', 0):,}</p>
                    <p style="margin: 0; color: #666;">Quality Validated</p>
                </div>

                <div style="text-align: center; padding: 15px; background: white; border-radius: 10px; border-left: 5px solid #FF9800;">
                    <h4 style="margin: 0; color: #FF9800;">DQX FAIL</h4>
                    <p style="font-size: 28px; margin: 5px 0; font-weight: bold;">{quality_dist.get('FAIL', 0):,}</p>
                    <p style="margin: 0; color: #666;">Quality Failed</p>
                </div>

                <div style="text-align: center; padding: 15px; background: white; border-radius: 10px; border-left: 5px solid {quality_status_color};">
                    <h4 style="margin: 0; color: {quality_status_color};">Quality Rate</h4>
                    <p style="font-size: 28px; margin: 5px 0; font-weight: bold;">{quality_rate:.1%}</p>
                    <p style="margin: 0; color: #666;">Success Rate</p>
                </div>
            </div>

            <!-- Pipeline Status -->
            <div style="background: #e3f2fd; padding: 15px; border-radius: 10px; margin-top: 20px;">
                <h4 style="margin: 0 0 10px 0;">🚀 Pipeline Status</h4>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
                    <div>
                        <strong>Streaming Status:</strong>
                        <span style="color: {streaming_status_color};">
                            {'🟢 Active' if streaming_status.get('is_active', False) else '🔴 Stopped'}
                        </span><br>
                        <strong>Batch ID:</strong> {streaming_status.get('batch_id', 'N/A')}<br>
                        <strong>Query ID:</strong> {streaming_status.get('query_id', 'N/A')[:8]}...
                    </div>
                    <div>
                        <strong>Avg Quality Score:</strong> {avg_score:.3f}<br>
                        <strong>Today's Records:</strong> {recent_records:,}<br>
                        <strong>Table:</strong> Purchase Orders DQX
                    </div>
                </div>
            </div>

            <!-- Quality Analysis -->
            <div style="background: #f3e5f5; padding: 15px; border-radius: 10px; margin-top: 15px;">
                <h4 style="margin: 0 0 10px 0;">📈 Quality Analysis</h4>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px;">
                    <div>
                        <strong>Quality Distribution:</strong><br>
                        PASS: {quality_dist.get('PASS', 0):,}<br>
                        FAIL: {quality_dist.get('FAIL', 0):,}<br>
                        WARNING: {quality_dist.get('WARNING', 0):,}
                    </div>
                    <div>
                        <strong>Performance:</strong><br>
                        Input Rate: {streaming_status.get('input_rows_per_second', 0):.1f} rows/sec<br>
                        Processing Rate: {streaming_status.get('processed_rows_per_second', 0):.1f} rows/sec
                    </div>
                    <div>
                        <strong>Health Check:</strong><br>
                        Status: {'✅ Healthy' if quality_rate >= 0.8 else '⚠️ Attention Needed'}<br>
                        Last Update: {datetime.now().strftime('%H:%M:%S')}
                    </div>
                </div>
            </div>
        </div>
        """

        return dashboard_html

    def _generate_no_data_html(self) -> str:
        """Generate HTML for when no data is available"""

        return f"""
        <div style="border: 2px solid #FF9800; border-radius: 15px; padding: 20px; background: #fff3e0; text-align: center;">
            <h2 style="color: #FF9800;">📭 Purchase Order DQX Pipeline</h2>
            <p style="font-size: 18px; color: #F57C00;">No data available yet in table: <code>{self.silver_table}</code></p>
            <p style="color: #666;">Pipeline may be starting up or no events have been processed.</p>
        </div>
        """

    def _display_console_dashboard(self, metrics: Dict[str, Any], streaming_status: Dict[str, Any]):
        """Display dashboard in console format"""

        print("=" * 60)
        print("🔍 PURCHASE ORDER DQX PIPELINE DASHBOARD")
        print("=" * 60)

        if not metrics.get('has_data', False):
            print("📭 No data available yet")
            print(f"   Table: {self.silver_table}")
            return

        print(f"📊 OVERVIEW:")
        print(f"   Total Records: {metrics['total_records']:,}")
        print(f"   Table: {self.silver_table}")
        print(f"   Quality Rate: {metrics['quality_rate']:.2%}")
        print(f"   Avg Quality Score: {metrics['avg_quality_score']:.3f}")

        print(f"\n📈 QUALITY DISTRIBUTION:")
        quality_dist = metrics['quality_distribution']
        print(f"   ✅ PASS: {quality_dist.get('PASS', 0):,} records")
        print(f"   ❌ FAIL: {quality_dist.get('FAIL', 0):,} records")
        print(f"   ⚠️ WARNING: {quality_dist.get('WARNING', 0):,} records")

        print(f"\n🚀 STREAMING STATUS:")
        is_active = streaming_status.get('is_active', False)
        print(f"   Status: {'🟢 Active' if is_active else '🔴 Stopped'}")
        print(f"   Batch ID: {streaming_status.get('batch_id', 'N/A')}")
        print(f"   Today's Records: {metrics['recent_records']:,}")

        # Show health status
        health_status = "✅ Healthy" if metrics['quality_rate'] >= 0.8 else "⚠️ Attention Needed"
        print(f"\n🏥 HEALTH STATUS: {health_status}")

        print("=" * 60)

    def _display_console_dashboard_fallback(self):
        """Fallback console display when metrics can't be retrieved"""

        print("=" * 60)
        print("🔍 PURCHASE ORDER DQX PIPELINE DASHBOARD")
        print("=" * 60)
        print("⚠️ Unable to retrieve current metrics")
        print(f"   Table: {self.silver_table}")
        print("   Please check if the pipeline is running and data is available")
        print("=" * 60)

    def analyze_quality_trends(self, days: int = 7):
        """
        Analyze quality trends over specified number of days.

        Args:
            days: Number of days to analyze
        """
        try:
            print(f"📊 Quality Trends Analysis - Last {days} Days")
            print("=" * 50)

            trends_df = self.spark.table(self.silver_table) \
                .filter(f"silver_processing_date >= date_sub(current_date(), {days})") \
                .groupBy("silver_processing_date") \
                .agg(
                    count("*").alias("total_records"),
                    sum(when(col("flag_check") == "PASS", 1).otherwise(0)).alias("pass_count"),
                    sum(when(col("flag_check") == "FAIL", 1).otherwise(0)).alias("fail_count"),
                    avg("dqx_quality_score").alias("avg_quality_score")
                ) \
                .withColumn("quality_rate", (col("pass_count") / col("total_records")) * 100) \
                .orderBy("silver_processing_date")

            trends_df.show(days, truncate=False)

            # Summary statistics
            total_days_with_data = trends_df.count()
            if total_days_with_data > 0:
                overall_stats = trends_df.agg(
                    sum("total_records").alias("total_records"),
                    avg("quality_rate").alias("avg_quality_rate"),
                    min("quality_rate").alias("min_quality_rate"),
                    max("quality_rate").alias("max_quality_rate")
                ).collect()[0]

                print(f"\n📈 TREND SUMMARY:")
                print(f"   Days with Data: {total_days_with_data}")
                print(f"   Total Records: {overall_stats['total_records']:,}")
                print(f"   Average Quality Rate: {overall_stats['avg_quality_rate']:.2f}%")
                print(f"   Quality Range: {overall_stats['min_quality_rate']:.2f}% - {overall_stats['max_quality_rate']:.2f}%")
            else:
                print("📭 No data found for the specified time period")

        except Exception as e:
            logger.error(f"❌ Error analyzing quality trends: {e}")

    def analyze_quality_failures(self, limit: int = 10):
        """
        Analyze quality failures in detail.

        Args:
            limit: Number of failure examples to show
        """
        try:
            print("🚫 Quality Failures Analysis")
            print("=" * 40)

            failed_count = self.spark.table(self.silver_table) \
                .filter("flag_check = 'FAIL'") \
                .count()

            if failed_count > 0:
                print(f"📊 Total Failed Records: {failed_count:,}")

                # Group by failure reasons
                print(f"\n📋 Top {limit} Failure Reasons:")
                self.spark.table(self.silver_table) \
                    .filter("flag_check = 'FAIL'") \
                    .groupBy("description_failure") \
                    .count() \
                    .orderBy(col("count").desc()) \
                    .limit(limit) \
                    .show(limit, truncate=False)

                # Show sample failed records
                print(f"\n📋 Sample Failed Records:")
                self.spark.table(self.silver_table) \
                    .filter("flag_check = 'FAIL'") \
                    .select("order_id", "product_id", "customer_id", "description_failure",
                           "dqx_quality_score", "silver_processing_timestamp") \
                    .orderBy(col("silver_processing_timestamp").desc()) \
                    .show(5, truncate=False)

            else:
                print("✅ No quality failures found - excellent data quality!")

        except Exception as e:
            logger.error(f"❌ Error analyzing quality failures: {e}")

    def track_dqx_lineage(self, limit: int = 10):
        """
        Track DQX lineage information.

        Args:
            limit: Number of lineage records to show
        """
        try:
            print("🔗 DQX Lineage Tracking")
            print("=" * 30)

            total_records = self.spark.table(self.silver_table).count()

            if total_records > 0:
                print(f"📊 Total Records with Lineage: {total_records:,}")

                # Show sample lineage records
                print(f"\n📋 Sample DQX Lineage Records:")
                self.spark.table(self.silver_table) \
                    .select("dqx_lineage_id", "order_id", "flag_check",
                           "dqx_quality_score", "dqx_validation_timestamp") \
                    .orderBy("dqx_validation_timestamp") \
                    .show(limit, truncate=False)

                # Lineage by criticality level
                print(f"\n📊 Lineage by Criticality Level:")
                self.spark.table(self.silver_table) \
                    .groupBy("dqx_criticality_level") \
                    .count() \
                    .orderBy("dqx_criticality_level") \
                    .show()

            else:
                print("📭 No lineage data found")

        except Exception as e:
            logger.error(f"❌ Error tracking DQX lineage: {e}")

    def monitor_streaming_progress(self, streaming_query: StreamingQuery, duration: int = 300):
        """
        Monitor streaming progress for specified duration.

        Args:
            streaming_query: Active streaming query
            duration: Monitoring duration in seconds
        """
        if not streaming_query:
            print("❌ No streaming query provided")
            return

        print(f"🔍 Monitoring streaming progress for {duration} seconds...")
        print("=" * 60)

        start_time = time.time()
        iteration = 1

        try:
            while time.time() - start_time < duration:
                if streaming_query.isActive:
                    print(f"\n🔄 Monitoring Iteration {iteration}")
                    print(f"   Time: {datetime.now().strftime('%H:%M:%S')}")

                    # Display dashboard
                    self.display_quality_dashboard(streaming_query)

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
                        logger.warning(f"⚠️ Error getting progress: {e}")

                    time.sleep(self.config.refresh_interval)
                    iteration += 1
                else:
                    print("❌ Streaming query is not active")
                    break

            print(f"\n✅ Monitoring completed after {duration} seconds")

        except KeyboardInterrupt:
            print("\n⏹️ Monitoring interrupted by user")
        except Exception as e:
            logger.error(f"❌ Monitoring error: {e}")

    def get_monitoring_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive monitoring summary.

        Returns:
            Dictionary with monitoring summary
        """
        try:
            metrics = self._get_current_metrics()

            if not metrics.get('has_data', False):
                return {'status': 'no_data', 'table': self.silver_table}

            # Calculate additional insights
            quality_rate = metrics['quality_rate']
            quality_status = "excellent" if quality_rate >= 0.95 else \
                           "good" if quality_rate >= 0.8 else \
                           "needs_attention" if quality_rate >= 0.6 else "critical"

            recommendations = []
            if quality_rate < 0.8:
                recommendations.append("Review and adjust DQX quality rules")
            if metrics['quality_distribution'].get('FAIL', 0) > 100:
                recommendations.append("Investigate high failure count")
            if metrics['avg_quality_score'] < 0.7:
                recommendations.append("Examine data source quality")

            return {
                'table': self.silver_table,
                'total_records': metrics['total_records'],
                'quality_rate': quality_rate,
                'quality_status': quality_status,
                'avg_quality_score': metrics['avg_quality_score'],
                'recent_records': metrics['recent_records'],
                'quality_distribution': metrics['quality_distribution'],
                'recommendations': recommendations,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error getting monitoring summary: {e}")
            return {'error': str(e), 'table': self.silver_table}