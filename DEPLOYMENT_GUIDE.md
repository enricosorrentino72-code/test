# Event Hub to Delta Lake Pipeline - Deployment Guide

This guide provides step-by-step instructions for deploying and running the Azure Event Hub to Delta Lake data ingestion pipeline.

## Prerequisites

### Software Requirements
- Python 3.8 or higher
- Azure CLI (for infrastructure deployment)
- Git (for version control)

### Azure Requirements
- Azure subscription with appropriate permissions
- Resource group (will be created if not exists)
- Sufficient quota for:
  - Event Hub namespace
  - Storage account (ADLS Gen2)
  - Databricks workspace (optional)
  - Key Vault

## Quick Start

### 1. Clone and Setup
```bash
git clone <repository-url>
cd azure-eventhub-delta-pipeline

# Install dependencies
pip install -r requirements.txt

# Run basic functionality tests
python test_basic_functionality.py
```

### 2. Local Testing (No Azure Resources Required)
```bash
# Test pipeline components locally
python test_local_pipeline.py

# Generate sample events for testing
python sample_event_generator.py
```

### 3. Deploy Infrastructure
```bash
# Login to Azure
az login

# Deploy all Azure resources
python deploy.py infrastructure
```

### 4. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your Azure credentials and connection strings
# (Values will be provided by the infrastructure deployment)
```

### 5. Run Pipeline
```bash
# For local Spark testing
python src/main.py

# For Databricks, upload and run the notebook:
# notebooks/EventHub_Delta_Pipeline.py
```

## Detailed Deployment Steps

### Step 1: Infrastructure Deployment

The ARM template (`infrastructure/arm-template.json`) creates:

1. **Event Hub Namespace and Event Hub**
   - Standard tier with 4 partitions
   - 7-day message retention
   - Capture enabled to blob storage

2. **Azure Data Lake Storage Gen2**
   - Hierarchical namespace enabled
   - Containers for Delta tables and Event Hub capture

3. **Azure Key Vault**
   - For storing connection strings and secrets
   - RBAC enabled

4. **Azure Databricks Workspace** (Optional)
   - Standard tier for Spark processing

#### Deploy Infrastructure:
```bash
# Set variables
RESOURCE_GROUP="eventhub-delta-pipeline-rg"
LOCATION="eastus"

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Deploy ARM template
az deployment group create \
  --resource-group $RESOURCE_GROUP \
  --template-file infrastructure/arm-template.json \
  --parameters projectName="my-pipeline"
```

#### Get Connection Strings:
```bash
# Event Hub connection string
az eventhubs namespace authorization-rule keys list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name <eventhub-namespace> \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv

# Storage account key
az storage account keys list \
  --resource-group $RESOURCE_GROUP \
  --account-name <storage-account> \
  --query [0].value -o tsv
```

### Step 2: Environment Configuration

Update the `.env` file with your Azure resources:

```bash
# Event Hub Configuration
EVENTHUB_CONNECTION_STRING=Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key
EVENTHUB_EVENT_HUB_NAME=events
EVENTHUB_CONSUMER_GROUP=$Default

# Azure Data Lake Storage Configuration
ADLS_ACCOUNT_NAME=your-storage-account
ADLS_CONTAINER_NAME=delta-tables
ADLS_ACCESS_KEY=your-storage-key

# Spark Configuration (adjust for your environment)
SPARK_MASTER=local[*]  # Use local[*] for local testing
SPARK_MAX_OFFSETS_PER_TRIGGER=1000
SPARK_TRIGGER_INTERVAL=30 seconds

# Monitoring Configuration
MONITORING_LOG_LEVEL=INFO
MONITORING_METRICS_PORT=8080
MONITORING_HEALTH_CHECK_PORT=8081
```

### Step 3: Testing the Pipeline

#### Local Testing (Recommended First)
```bash
# Test with sample data (no Azure resources needed)
python test_local_pipeline.py

# Generate test events
python sample_event_generator.py
```

#### Integration Testing with Azure Resources
```bash
# Send test events to Event Hub
python -c "
from sample_event_generator import SampleEventGenerator
from azure.eventhub import EventHubProducerClient, EventData
import json
import os

# Generate sample events
generator = SampleEventGenerator()
events = generator.generate_batch(count=10)

# Send to Event Hub
connection_str = os.getenv('EVENTHUB_CONNECTION_STRING')
eventhub_name = os.getenv('EVENTHUB_EVENT_HUB_NAME')

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name
)

with producer:
    event_data_batch = producer.create_batch()
    for event in events:
        event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)

print(f'Sent {len(events)} events to Event Hub')
"
```

### Step 4: Running the Pipeline

#### Option A: Local Spark
```bash
# Run the main pipeline
python src/main.py
```

#### Option B: Databricks
1. Upload the notebook `notebooks/EventHub_Delta_Pipeline.py` to Databricks
2. Create a cluster with Delta Lake libraries
3. Configure the notebook with your connection strings
4. Run the notebook

#### Option C: Azure Synapse Analytics
1. Create a Synapse workspace
2. Import the pipeline code
3. Configure Spark pools
4. Run as a Synapse pipeline

### Step 5: Monitoring and Maintenance

#### Health Checks
```bash
# Check pipeline health
curl http://localhost:8081/health

# Check readiness
curl http://localhost:8081/health/ready

# Check liveness
curl http://localhost:8081/health/live
```

#### Metrics
```bash
# View Prometheus metrics
curl http://localhost:8080/metrics
```

#### Delta Table Operations
```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("maintenance").getOrCreate()

# View table history
table_path = "abfss://delta-tables@youraccount.dfs.core.windows.net/delta-tables/events/events"
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().show()

# Optimize table
delta_table.optimize().executeCompaction()

# Vacuum old files (7 days retention)
delta_table.vacuum(168)  # 168 hours = 7 days
```

## Configuration Options

### Spark Configuration
- `SPARK_MASTER`: Spark master URL (local[*] for local, cluster URL for distributed)
- `SPARK_MAX_OFFSETS_PER_TRIGGER`: Max events per batch
- `SPARK_TRIGGER_INTERVAL`: How often to process batches

### Event Hub Configuration
- `EVENTHUB_CONSUMER_GROUP`: Consumer group name (use different groups for multiple consumers)
- `EVENTHUB_CHECKPOINT_LOCATION`: Where to store streaming checkpoints

### Delta Lake Configuration
- `ADLS_DELTA_TABLE_PATH`: Path within container for Delta tables
- Partitioning: Currently by date and hour, can be customized

### Monitoring Configuration
- `MONITORING_LOG_LEVEL`: DEBUG, INFO, WARNING, ERROR
- `MONITORING_ENABLE_METRICS`: Enable/disable Prometheus metrics
- Ports: Customize metrics and health check ports

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Event Hub connection string
   - Check network connectivity
   - Ensure proper authentication

2. **Permission Errors**
   - Verify Azure RBAC permissions
   - Check storage account access keys
   - Ensure managed identity is configured

3. **Spark Errors**
   - Check Spark cluster resources
   - Verify Delta Lake libraries are installed
   - Review checkpoint location permissions

4. **Performance Issues**
   - Adjust `maxEventsPerTrigger`
   - Optimize Delta table partitioning
   - Scale Spark cluster resources

### Debugging

1. **Enable Debug Logging**
   ```bash
   export MONITORING_LOG_LEVEL=DEBUG
   python src/main.py
   ```

2. **Check Streaming Query Status**
   ```python
   # In Spark/Databricks
   spark.streams.active  # List active queries
   query.lastProgress    # Check last batch progress
   query.exception       # Check for errors
   ```

3. **Validate Event Data**
   ```python
   # Read from Delta table
   df = spark.read.format("delta").load(table_path)
   df.show()
   df.printSchema()
   ```

## Security Best Practices

1. **Use Managed Identity** (Production)
   - Enable managed identity for Databricks/Synapse
   - Avoid storing secrets in code

2. **Key Vault Integration**
   - Store connection strings in Key Vault
   - Use Key Vault references in configuration

3. **Network Security**
   - Use private endpoints for Event Hub and Storage
   - Configure firewall rules

4. **Access Control**
   - Use RBAC for resource access
   - Implement least privilege principle

## Performance Optimization

1. **Event Hub**
   - Use appropriate partition count
   - Configure capture for backup

2. **Spark**
   - Tune cluster size and configuration
   - Optimize batch size and trigger interval

3. **Delta Lake**
   - Use appropriate partitioning strategy
   - Regular optimization and vacuuming
   - Z-ordering for query performance

## Scaling Considerations

1. **Horizontal Scaling**
   - Multiple Event Hub partitions
   - Multiple Spark executors
   - Parallel processing

2. **Vertical Scaling**
   - Larger Spark cluster nodes
   - More memory and CPU

3. **Auto-scaling**
   - Databricks auto-scaling clusters
   - Event Hub auto-inflate

## Cost Optimization

1. **Event Hub**
   - Choose appropriate tier
   - Monitor throughput units

2. **Storage**
   - Use appropriate storage tier
   - Implement lifecycle policies

3. **Compute**
   - Use spot instances where appropriate
   - Auto-scaling and auto-termination

## Support and Maintenance

### Regular Tasks
- Monitor pipeline health and performance
- Optimize and vacuum Delta tables
- Review and rotate access keys
- Update dependencies and security patches

### Monitoring Dashboards
- Set up Azure Monitor dashboards
- Configure alerts for failures
- Track key performance metrics

### Backup and Recovery
- Event Hub capture provides backup
- Delta Lake time travel for recovery
- Regular testing of disaster recovery procedures
