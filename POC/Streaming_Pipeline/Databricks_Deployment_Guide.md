# Databricks Event Hub Producer Deployment Guide

## Overview
This guide covers deploying the Event Hub weather data producer in Azure Databricks workspace using notebooks.

## Prerequisites

### Azure Resources
- **Azure Databricks Workspace** (Premium tier recommended)
- **Azure Event Hub Namespace** with Standard/Premium tier
- **Event Hub** created with appropriate partitions
- **Service Principal** or **Managed Identity** with Event Hub Data Sender permissions

### Databricks Requirements
- **Databricks Runtime**: 12.2 LTS or higher (includes Python 3.9+)
- **Cluster Configuration**: 
  - Single node or multi-node cluster
  - Minimum: 1 driver node (4 cores, 16GB RAM)
  - Recommended: Auto-scaling enabled

## Setup Instructions

### 1. Create Databricks Secret Scope

#### Option A: Databricks-backed Secret Scope
```bash
# Using Databricks CLI
databricks secrets create-scope --scope eventhub-secrets

# Add Event Hub connection string
databricks secrets put --scope eventhub-secrets --key eventhub-connection-string
```

#### Option B: Azure Key Vault-backed Secret Scope (Recommended)
```bash
# Link to Azure Key Vault
databricks secrets create-scope --scope eventhub-secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/{subscription-id}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{vault-name} \
  --dns-name https://{vault-name}.vault.azure.net/
```

### 2. Configure Cluster Libraries

#### Install Required Libraries on Cluster
Navigate to **Clusters** → Select your cluster → **Libraries** → **Install New**

**PyPI Libraries to Install:**
```
azure-eventhub>=5.11.0
backoff>=2.2.1
pydantic>=2.0.0
```

#### Alternative: Install via Notebook Cell
The notebook includes a `%pip install` command that will install libraries automatically.

### 3. Import and Configure Notebook

#### Import Notebook
1. Go to **Workspace** → **Import**
2. Select **File** and upload `EventHub_Producer_Databricks.py`
3. Choose **Python** as the language
4. Select target folder (e.g., `/Shared/EventHub/`)

#### Configure Widgets
The notebook includes interactive widgets for configuration:

| Widget | Default | Description |
|--------|---------|-------------|
| `eventhub_scope` | "eventhub-secrets" | Secret scope name |
| `eventhub_name` | "weather-events" | Event Hub name |
| `batch_size` | "50" | Events per batch |
| `send_interval` | "2.0" | Seconds between batches |
| `duration_minutes` | "60" | Total run duration |
| `log_level` | "INFO" | Logging verbosity |

### 4. Event Hub Configuration

#### Create Event Hub
```bash
# Create Event Hub namespace
az eventhubs namespace create \
  --resource-group myResourceGroup \
  --name walgreens-eventhub-ns \
  --location eastus \
  --sku Standard

# Create Event Hub
az eventhubs eventhub create \
  --resource-group myResourceGroup \
  --namespace-name walgreens-eventhub-ns \
  --name weather-events \
  --partition-count 16 \
  --message-retention 7
```

#### Configure Access Policy
```bash
# Create access policy with Send permissions
az eventhubs eventhub authorization-rule create \
  --resource-group myResourceGroup \
  --namespace-name walgreens-eventhub-ns \
  --eventhub-name weather-events \
  --name databricks-producer \
  --rights Send

# Get connection string
az eventhubs eventhub authorization-rule keys list \
  --resource-group myResourceGroup \
  --namespace-name walgreens-eventhub-ns \
  --eventhub-name weather-events \
  --name databricks-producer
```

## Production Deployment

### 1. Notebook Scheduling

#### Create Databricks Job
```json
{
  "name": "EventHub-Weather-Producer",
  "new_cluster": {
    "spark_version": "12.2.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 0,
    "custom_tags": {
      "project": "weather-streaming",
      "environment": "production"
    }
  },
  "notebook_task": {
    "notebook_path": "/Shared/EventHub/EventHub_Producer_Databricks",
    "base_parameters": {
      "eventhub_name": "weather-events-prod",
      "batch_size": "100",
      "send_interval": "1.0",
      "duration_minutes": "1440"
    }
  },
  "timeout_seconds": 86400,
  "max_retries": 3,
  "email_notifications": {
    "on_failure": ["admin@company.com"],
    "on_success": ["admin@company.com"]
  }
}
```

#### Schedule Job
```bash
# Create scheduled job (daily at 6 AM)
databricks jobs create --json-file job-config.json

# Update job schedule
databricks jobs update --job-id 123 --json '{
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "UTC"
  }
}'
```

### 2. High-Availability Setup

#### Multi-Region Deployment
```python
# Configure multiple Event Hub endpoints
EVENTHUB_CONFIGS = [
    {
        "region": "eastus",
        "connection_string": dbutils.secrets.get("eventhub-secrets", "eastus-connection"),
        "eventhub_name": "weather-events-east"
    },
    {
        "region": "westus",
        "connection_string": dbutils.secrets.get("eventhub-secrets", "westus-connection"),
        "eventhub_name": "weather-events-west"
    }
]
```

#### Auto-scaling Configuration
```json
{
  "autoscale": {
    "min_workers": 1,
    "max_workers": 8
  },
  "auto_termination_minutes": 60
}
```

### 3. Monitoring and Alerting

#### Custom Metrics
```python
# Add custom metrics to track performance
def log_custom_metrics(stats):
    dbutils.metrics.log_metric("messages_sent", stats['messages_sent'])
    dbutils.metrics.log_metric("error_rate", stats['errors'] / stats['batches_sent'])
    dbutils.metrics.log_metric("throughput", stats['messages_sent'] / runtime)
```

#### Azure Monitor Integration
```python
# Send telemetry to Azure Monitor
from azure.monitor.opentelemetry import configure_azure_monitor
configure_azure_monitor(connection_string="InstrumentationKey=your-key")
```

## Configuration Best Practices

### 1. Production Settings

#### Optimized Configuration
```python
# High throughput configuration
PRODUCTION_CONFIG = {
    "batch_size": 200,           # Larger batches for efficiency
    "send_interval": 0.5,        # Faster sending for real-time
    "duration_minutes": 1440,    # Run for 24 hours
    "log_level": "WARNING"       # Reduce log verbosity
}
```

#### Cluster Sizing
```
Small workload:  1 node  (4 cores, 16GB) - ~1K msg/sec
Medium workload: 2 nodes (8 cores, 32GB) - ~5K msg/sec  
Large workload:  4 nodes (16 cores, 64GB) - ~10K msg/sec
```

### 2. Cost Optimization

#### Spot Instances
```json
{
  "cluster_source": "JOB",
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK"
  }
}
```

#### Auto-termination
```python
# Set automatic cluster termination
spark.conf.set("spark.databricks.cluster.terminateInactiveTimeoutMinutes", "30")
```

### 3. Security Configuration

#### Network Security
```json
{
  "custom_tags": {
    "security": "high"
  },
  "enable_elastic_disk": false,
  "init_scripts": [{
    "dbfs": {
      "destination": "dbfs:/security/init-script.sh"
    }
  }]
}
```

#### Secret Management Best Practices
- Use Azure Key Vault-backed secret scopes
- Rotate access keys regularly
- Apply principle of least privilege
- Enable audit logging for secret access

## Troubleshooting

### Common Issues

#### 1. Connection Failures
```
Error: "The messaging entity could not be found"
Solution: Verify Event Hub name and namespace in connection string
```

#### 2. Authentication Errors
```
Error: "Unauthorized access"
Solution: Check access policy permissions (Send permission required)
```

#### 3. Library Installation Issues
```
Error: "Module 'azure.eventhub' has no attribute 'EventHubProducerClient'"
Solution: Restart cluster after installing libraries
```

#### 4. Performance Issues
```
Symptom: Low throughput (< 100 msg/sec)
Solutions:
- Increase batch_size to 100-200
- Decrease send_interval to 0.5-1.0 seconds
- Check Event Hub throughput units
- Scale cluster resources
```

### Debugging Commands

#### Check Cluster Libraries
```python
import pkg_resources
installed_packages = [d.project_name for d in pkg_resources.working_set]
print("azure-eventhub" in installed_packages)
```

#### Validate Secrets
```python
try:
    connection_string = dbutils.secrets.get("eventhub-secrets", "eventhub-connection-string")
    print("✅ Secret retrieved successfully")
    print(f"Connection string length: {len(connection_string)}")
except Exception as e:
    print(f"❌ Secret retrieval failed: {e}")
```

#### Test Event Hub Connectivity
```python
def test_connection():
    try:
        from azure.eventhub import EventHubProducerClient
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            eventhub_name="weather-events"
        )
        producer.close()
        return "✅ Connection successful"
    except Exception as e:
        return f"❌ Connection failed: {e}"

print(test_connection())
```

## Performance Optimization

### 1. Throughput Tuning

#### Optimal Configuration Matrix
| Scenario | Batch Size | Send Interval | Expected Throughput |
|----------|------------|---------------|-------------------|
| Real-time | 50 | 0.5s | ~6K msg/min |
| High-throughput | 200 | 1.0s | ~12K msg/min |
| Balanced | 100 | 2.0s | ~3K msg/min |

#### Event Hub Partitioning
```python
# Optimize partition distribution
def get_partition_key(city: str) -> str:
    """Distribute events across partitions by city"""
    return city.lower().replace(" ", "")

# Configure partition count
RECOMMENDED_PARTITIONS = {
    "light_workload": 4,    # < 1K msg/sec
    "medium_workload": 8,   # 1-5K msg/sec  
    "heavy_workload": 16    # > 5K msg/sec
}
```

### 2. Resource Optimization

#### Memory Management
```python
# Optimize for memory usage
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

#### CPU Utilization
```python
# Parallelize batch generation
from concurrent.futures import ThreadPoolExecutor

def generate_parallel_batches(batch_count: int, batch_size: int):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(weather_generator.generate_weather_reading) 
            for _ in range(batch_count * batch_size)
        ]
        return [future.result() for future in futures]
```

## Disaster Recovery

### 1. Multi-Region Setup
```python
# Failover configuration
FAILOVER_CONFIG = {
    "primary_region": {
        "connection_string": "primary-connection-string",
        "eventhub_name": "weather-events-east"
    },
    "secondary_region": {
        "connection_string": "secondary-connection-string", 
        "eventhub_name": "weather-events-west"
    }
}
```

### 2. Data Backup Strategy
```python
# Save events to Delta Lake for backup
def backup_to_delta(batch_data):
    spark.createDataFrame(batch_data).write \
        .format("delta") \
        .mode("append") \
        .save("/mnt/backup/weather-events")
```

This comprehensive deployment guide ensures reliable, scalable, and maintainable Event Hub streaming in Databricks production environments.