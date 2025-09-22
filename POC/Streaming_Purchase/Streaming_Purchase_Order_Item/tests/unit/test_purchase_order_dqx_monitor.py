"""
Unit tests for PurchaseOrderDQXMonitor class.

Tests cover:
- Real-time monitoring
- Alert generation
- Metrics collection
- Dashboard updates
- Anomaly detection
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timedelta
import sys
import os
import json

# Add the class directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../class')))

from purchase_order_dqx_monitor import PurchaseOrderDQXMonitor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType


class TestPurchaseOrderDQXMonitor:
    """Test suite for PurchaseOrderDQXMonitor class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.sql = Mock()
        spark.readStream = Mock()
        spark.read = Mock()
        return spark

    @pytest.fixture
    def dqx_monitor(self, mock_spark):
        """Create a PurchaseOrderDQXMonitor instance for testing."""
        return PurchaseOrderDQXMonitor(
            spark=mock_spark,
            monitoring_table="dqx_monitoring",
            alert_threshold=0.95,
            log_level="INFO"
        )

    def test_initialization(self, dqx_monitor):
        """Test PurchaseOrderDQXMonitor initialization."""
        assert dqx_monitor.monitoring_table == "dqx_monitoring"
        assert dqx_monitor.alert_threshold == 0.95
        assert dqx_monitor.logger is not None
        assert dqx_monitor.metrics == {}

    def test_start_monitoring(self, dqx_monitor, mock_spark):
        """Test starting the monitoring process."""
        mock_stream = Mock()
        mock_spark.readStream.format = Mock(return_value=Mock(
            option=Mock(return_value=Mock(
                load=Mock(return_value=mock_stream)
            ))
        ))

        result = dqx_monitor.start_monitoring(
            source_path="/mnt/dqx/results",
            checkpoint_location="/mnt/checkpoints/monitor"
        )

        assert result is not None
        mock_spark.readStream.format.assert_called()

    def test_collect_metrics(self, dqx_monitor):
        """Test metrics collection."""
        mock_df = Mock(spec=DataFrame)
        mock_df.agg = Mock(return_value=Mock(
            collect=Mock(return_value=[
                Mock(asDict=Mock(return_value={
                    'total_records': 1000,
                    'passed_records': 950,
                    'failed_records': 50
                }))
            ])
        ))

        metrics = dqx_monitor.collect_metrics(mock_df)

        assert metrics['total_records'] == 1000
        assert metrics['passed_records'] == 950
        assert metrics['failed_records'] == 50
        assert metrics['pass_rate'] == 0.95

    def test_check_alert_conditions(self, dqx_monitor):
        """Test alert condition checking."""
        metrics = {
            'pass_rate': 0.90,
            'failed_records': 100,
            'anomaly_detected': False
        }

        alerts = dqx_monitor.check_alert_conditions(metrics)

        assert len(alerts) > 0
        assert any('below threshold' in alert for alert in alerts)

    def test_generate_alert(self, dqx_monitor):
        """Test alert generation."""
        with patch('purchase_order_dqx_monitor.send_notification') as mock_send:
            mock_send.return_value = True

            result = dqx_monitor.generate_alert(
                alert_type="QUALITY_THRESHOLD",
                message="Data quality below 95%",
                severity="HIGH",
                metrics={'pass_rate': 0.90}
            )

            assert result is True
            mock_send.assert_called_once()

    def test_update_dashboard(self, dqx_monitor, mock_spark):
        """Test dashboard updates."""
        metrics = {
            'timestamp': datetime.now(),
            'pass_rate': 0.95,
            'total_records': 1000
        }

        result = dqx_monitor.update_dashboard(metrics)

        assert result is True
        mock_spark.sql.assert_called()

    def test_detect_anomalies(self, dqx_monitor):
        """Test anomaly detection."""
        current_metrics = {
            'pass_rate': 0.70,  # Significant drop
            'processing_time': 500,  # High processing time
            'failed_records': 300
        }

        historical_metrics = {
            'avg_pass_rate': 0.95,
            'avg_processing_time': 100,
            'avg_failed_records': 50
        }

        anomalies = dqx_monitor.detect_anomalies(current_metrics, historical_metrics)

        assert len(anomalies) > 0
        assert any('pass_rate' in anomaly for anomaly in anomalies)

    def test_calculate_sla_compliance(self, dqx_monitor):
        """Test SLA compliance calculation."""
        metrics = {
            'pass_rate': 0.96,
            'processing_time': 90,
            'availability': 0.999
        }

        sla_targets = {
            'min_pass_rate': 0.95,
            'max_processing_time': 100,
            'min_availability': 0.99
        }

        compliance = dqx_monitor.calculate_sla_compliance(metrics, sla_targets)

        assert compliance['pass_rate'] is True
        assert compliance['processing_time'] is True
        assert compliance['availability'] is True
        assert compliance['overall'] is True

    def test_get_monitoring_summary(self, dqx_monitor, mock_spark):
        """Test retrieving monitoring summary."""
        mock_result = Mock()
        mock_result.collect = Mock(return_value=[
            Mock(asDict=Mock(return_value={
                'date': '2024-09-19',
                'total_records': 10000,
                'passed_records': 9500,
                'failed_records': 500
            }))
        ])
        mock_spark.sql.return_value = mock_result

        summary = dqx_monitor.get_monitoring_summary(
            start_date=datetime(2024, 9, 19),
            end_date=datetime(2024, 9, 20)
        )

        assert summary is not None
        assert len(summary) == 1
        assert summary[0]['total_records'] == 10000

    def test_export_monitoring_report(self, dqx_monitor):
        """Test exporting monitoring report."""
        with patch('builtins.open', create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            metrics = {
                'timestamp': datetime.now().isoformat(),
                'pass_rate': 0.95,
                'total_records': 1000
            }

            result = dqx_monitor.export_monitoring_report(
                metrics=metrics,
                filepath="/tmp/report.json"
            )

            assert result is True
            mock_open.assert_called_once_with("/tmp/report.json", 'w')

    def test_create_monitoring_checkpoint(self, dqx_monitor):
        """Test creating monitoring checkpoint."""
        result = dqx_monitor.create_monitoring_checkpoint(
            checkpoint_name="monitor_checkpoint_20240919",
            metrics={'pass_rate': 0.95}
        )

        assert result is True
        assert 'monitor_checkpoint_20240919' in dqx_monitor.checkpoints

    def test_rollback_to_checkpoint(self, dqx_monitor):
        """Test rolling back to a checkpoint."""
        # First create a checkpoint
        dqx_monitor.checkpoints['test_checkpoint'] = {
            'metrics': {'pass_rate': 0.95},
            'timestamp': datetime.now()
        }

        result = dqx_monitor.rollback_to_checkpoint('test_checkpoint')

        assert result is True
        assert dqx_monitor.metrics['pass_rate'] == 0.95

    def test_monitoring_health_check(self, dqx_monitor):
        """Test monitoring system health check."""
        health_status = dqx_monitor.health_check()

        assert health_status['status'] in ['healthy', 'degraded', 'unhealthy']
        assert 'components' in health_status
        assert 'timestamp' in health_status