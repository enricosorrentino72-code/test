import pytest
import os
from unittest.mock import patch

from src.config.settings import (
    EventHubSettings, ADLSSettings, SparkSettings, 
    MonitoringSettings, AzureSettings, PipelineSettings
)


class TestEventHubSettings:
    """Test EventHub configuration settings."""
    
    def test_default_values(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {
            'EVENTHUB_CONNECTION_STRING': 'test-connection-string',
            'EVENTHUB_EVENT_HUB_NAME': 'test-hub'
        }):
            settings = EventHubSettings()
            assert settings.connection_string == 'test-connection-string'
            assert settings.event_hub_name == 'test-hub'
            assert settings.consumer_group == '$Default'
            assert settings.checkpoint_location == '/tmp/eventhub-checkpoints'
    
    def test_custom_values(self):
        """Test custom configuration values."""
        with patch.dict(os.environ, {
            'EVENTHUB_CONNECTION_STRING': 'custom-connection-string',
            'EVENTHUB_EVENT_HUB_NAME': 'custom-hub',
            'EVENTHUB_CONSUMER_GROUP': 'custom-group',
            'EVENTHUB_CHECKPOINT_LOCATION': '/custom/checkpoint'
        }):
            settings = EventHubSettings()
            assert settings.connection_string == 'custom-connection-string'
            assert settings.event_hub_name == 'custom-hub'
            assert settings.consumer_group == 'custom-group'
            assert settings.checkpoint_location == '/custom/checkpoint'


class TestADLSSettings:
    """Test ADLS configuration settings."""
    
    def test_default_values(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {
            'ADLS_ACCOUNT_NAME': 'test-account',
            'ADLS_CONTAINER_NAME': 'test-container'
        }):
            settings = ADLSSettings()
            assert settings.account_name == 'test-account'
            assert settings.container_name == 'test-container'
            assert settings.delta_table_path == 'delta-tables/events'
            assert settings.access_key is None


class TestSparkSettings:
    """Test Spark configuration settings."""
    
    def test_default_values(self):
        """Test default configuration values."""
        settings = SparkSettings()
        assert settings.app_name == 'EventHub-Delta-Pipeline'
        assert settings.master == 'local[*]'
        assert settings.max_offsets_per_trigger == 1000
        assert settings.trigger_interval == '30 seconds'
    
    def test_custom_values(self):
        """Test custom configuration values."""
        with patch.dict(os.environ, {
            'SPARK_APP_NAME': 'Custom-App',
            'SPARK_MASTER': 'spark://localhost:7077',
            'SPARK_MAX_OFFSETS_PER_TRIGGER': '2000',
            'SPARK_TRIGGER_INTERVAL': '60 seconds'
        }):
            settings = SparkSettings()
            assert settings.app_name == 'Custom-App'
            assert settings.master == 'spark://localhost:7077'
            assert settings.max_offsets_per_trigger == 2000
            assert settings.trigger_interval == '60 seconds'


class TestMonitoringSettings:
    """Test monitoring configuration settings."""
    
    def test_default_values(self):
        """Test default configuration values."""
        settings = MonitoringSettings()
        assert settings.log_level == 'INFO'
        assert settings.metrics_port == 8080
        assert settings.health_check_port == 8081
        assert settings.enable_metrics == True


class TestAzureSettings:
    """Test Azure configuration settings."""
    
    def test_default_values(self):
        """Test default configuration values."""
        settings = AzureSettings()
        assert settings.tenant_id is None
        assert settings.client_id is None
        assert settings.client_secret is None
        assert settings.key_vault_url is None
        assert settings.use_managed_identity == True


class TestPipelineSettings:
    """Test main pipeline configuration."""
    
    def test_initialization(self):
        """Test pipeline settings initialization."""
        with patch.dict(os.environ, {
            'EVENTHUB_CONNECTION_STRING': 'test-connection',
            'EVENTHUB_EVENT_HUB_NAME': 'test-hub',
            'ADLS_ACCOUNT_NAME': 'test-account',
            'ADLS_CONTAINER_NAME': 'test-container'
        }):
            settings = PipelineSettings()
            
            assert isinstance(settings.eventhub, EventHubSettings)
            assert isinstance(settings.adls, ADLSSettings)
            assert isinstance(settings.spark, SparkSettings)
            assert isinstance(settings.monitoring, MonitoringSettings)
            assert isinstance(settings.azure, AzureSettings)
            
            assert settings.eventhub.connection_string == 'test-connection'
            assert settings.adls.account_name == 'test-account'
