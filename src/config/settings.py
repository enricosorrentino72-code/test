from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EventHubSettings(BaseSettings):
    """Event Hub configuration settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="EVENTHUB_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    connection_string: str = Field(..., description="Event Hub connection string")
    consumer_group: str = Field(default="$Default", description="Consumer group name")
    event_hub_name: str = Field(..., description="Event Hub name")
    checkpoint_location: str = Field(
        default="/tmp/eventhub-checkpoints",
        description="Checkpoint location for streaming"
    )


class ADLSSettings(BaseSettings):
    """Azure Data Lake Storage configuration settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="ADLS_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    account_name: str = Field(..., description="Storage account name")
    container_name: str = Field(..., description="Container name")
    delta_table_path: str = Field(
        default="delta-tables/events",
        description="Path to Delta table in ADLS"
    )
    access_key: Optional[str] = Field(default=None, description="Storage account access key")


class SparkSettings(BaseSettings):
    """Spark configuration settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    app_name: str = Field(default="EventHub-Delta-Pipeline", description="Spark application name")
    master: str = Field(default="local[*]", description="Spark master URL")
    max_offsets_per_trigger: int = Field(
        default=1000,
        description="Maximum number of offsets to process per trigger"
    )
    trigger_interval: str = Field(
        default="30 seconds",
        description="Streaming trigger interval"
    )


class MonitoringSettings(BaseSettings):
    """Monitoring and logging configuration settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="MONITORING_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    log_level: str = Field(default="INFO", description="Logging level")
    metrics_port: int = Field(default=8080, description="Prometheus metrics port")
    health_check_port: int = Field(default=8081, description="Health check port")
    enable_metrics: bool = Field(default=True, description="Enable Prometheus metrics")


class AzureSettings(BaseSettings):
    """Azure-specific configuration settings."""
    
    model_config = SettingsConfigDict(
        env_prefix="AZURE_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    tenant_id: Optional[str] = Field(default=None, description="Azure tenant ID")
    client_id: Optional[str] = Field(default=None, description="Azure client ID")
    client_secret: Optional[str] = Field(default=None, description="Azure client secret")
    key_vault_url: Optional[str] = Field(default=None, description="Azure Key Vault URL")
    use_managed_identity: bool = Field(
        default=True,
        description="Use managed identity for authentication"
    )


class PipelineSettings(BaseSettings):
    """Main pipeline configuration that combines all settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    eventhub: EventHubSettings = Field(default_factory=EventHubSettings)
    adls: ADLSSettings = Field(default_factory=ADLSSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    azure: AzureSettings = Field(default_factory=AzureSettings)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.eventhub = EventHubSettings()
        self.adls = ADLSSettings()
        self.spark = SparkSettings()
        self.monitoring = MonitoringSettings()
        self.azure = AzureSettings()


def get_settings() -> PipelineSettings:
    """Get pipeline settings instance."""
    return PipelineSettings()
