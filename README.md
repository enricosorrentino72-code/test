# Azure Event Hub to Delta Lake Pipeline

A data ingestion pipeline that captures events from Azure Event Hub and stores them in Azure Data Lake Storage Gen2 (ADLS) using Delta Tables with Apache Spark.

## Architecture Overview

This pipeline consists of the following components:

1. **Event Hub Consumer Module**: Spark Structured Streaming job to consume events from Event Hub
2. **Data Processing Layer**: Data transformation, cleansing, validation, and enrichment
3. **Delta Lake Integration**: Upsert/merge operations with optimized partitioning
4. **Monitoring & Alerting**: Logging, metrics, health checks, and performance monitoring

## Project Structure

```
azure-eventhub-delta-pipeline/
├── src/
│   ├── consumer/           # Event Hub consumer components
│   ├── processing/         # Data transformation logic
│   ├── delta/             # Delta Lake operations
│   ├── monitoring/        # Logging and monitoring
│   └── config/            # Configuration management
├── infrastructure/        # ARM templates/Terraform
├── tests/                # Unit and integration tests
├── notebooks/            # Databricks/Synapse notebooks
├── requirements.txt      # Python dependencies
└── setup.py             # Package setup
```

## Technologies Used

- **Apache Spark**: PySpark for distributed processing
- **Delta Lake**: For ACID transactions and data versioning
- **Azure Event Hub**: Event streaming platform
- **Azure Data Lake Storage Gen2**: Scalable data storage
- **Azure Key Vault**: Secrets management
- **Azure Databricks/Synapse**: Spark runtime environment

## Getting Started

1. Install dependencies: `pip install -r requirements.txt`
2. Configure Azure credentials and connection strings
3. Run the pipeline: `python src/main.py`

## Configuration

The pipeline uses environment variables and Azure Key Vault for configuration:

- `EVENTHUB_CONNECTION_STRING`: Event Hub connection string
- `ADLS_ACCOUNT_NAME`: Storage account name
- `ADLS_CONTAINER_NAME`: Container name for Delta tables
- `KEY_VAULT_URL`: Azure Key Vault URL for secrets

## Monitoring

The pipeline includes comprehensive monitoring with:
- Structured logging with correlation IDs
- Metrics collection for throughput and latency
- Health check endpoints
- Dead letter queue for failed events
