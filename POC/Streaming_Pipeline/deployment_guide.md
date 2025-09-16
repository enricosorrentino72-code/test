# Event Hub Producer Deployment Guide

## Overview
This guide covers deploying the improved Event Hub weather data producer in production environments.

## Prerequisites

### Azure Resources
- **Azure Event Hub Namespace** with appropriate tier (Standard/Premium for production)
- **Event Hub** configured with desired partitions and retention
- **Service Principal** or **Managed Identity** with Event Hub Data Sender permissions

### Environment Requirements
- Python 3.8+
- Network connectivity to Azure Event Hub endpoints
- Appropriate firewall/security group configurations

## Configuration

### Environment Variables
Set the following environment variables:

```bash
# Required
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=your-policy;SharedAccessKey=your-key"
export EVENTHUB_NAME="your-eventhub-name"

# Optional - Performance Tuning
export BATCH_SIZE="50"                    # Events per batch (default: 10)
export SEND_INTERVAL_SECONDS="2.0"       # Seconds between batches (default: 5.0)
export MAX_RETRIES="5"                    # Retry attempts (default: 3)
export RETRY_BACKOFF_SECONDS="2.0"       # Backoff delay (default: 1.0)
```

### Production Configuration Recommendations

#### Event Hub Settings
```
Partitions: 8-32 (based on expected throughput)
Message Retention: 7 days (minimum for production)
Throughput Units: Auto-scale enabled
```

#### Application Settings
```
BATCH_SIZE=100                 # Higher batches for better throughput
SEND_INTERVAL_SECONDS=1.0      # Faster sending for real-time needs
MAX_RETRIES=5                  # More resilient error handling
```

## Deployment Options

### 1. Docker Container Deployment

Create `Dockerfile`:
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY improved_eventhub_producer.py .

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Run the producer
CMD ["python", "improved_eventhub_producer.py"]
```

Build and run:
```bash
# Build image
docker build -t eventhub-producer:latest .

# Run container with environment variables
docker run -d \
  --name weather-producer \
  --restart unless-stopped \
  -e EVENTHUB_CONNECTION_STRING="your-connection-string" \
  -e EVENTHUB_NAME="weather-events" \
  -e BATCH_SIZE="50" \
  -v /var/log/app:/app/logs \
  eventhub-producer:latest
```

### 2. Kubernetes Deployment

Create `k8s-deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventhub-weather-producer
  labels:
    app: weather-producer
spec:
  replicas: 2
  selector:
    matchLabels:
      app: weather-producer
  template:
    metadata:
      labels:
        app: weather-producer
    spec:
      containers:
      - name: producer
        image: eventhub-producer:latest
        env:
        - name: EVENTHUB_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: eventhub-secret
              key: connection-string
        - name: EVENTHUB_NAME
          value: "weather-events"
        - name: BATCH_SIZE
          value: "50"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          exec:
            command:
            - pgrep
            - python
          initialDelaySeconds: 30
          periodSeconds: 30
---
apiVersion: v1
kind: Secret
metadata:
  name: eventhub-secret
type: Opaque
data:
  connection-string: <base64-encoded-connection-string>
```

Deploy:
```bash
kubectl apply -f k8s-deployment.yaml
```

### 3. Azure Container Instances

```bash
az container create \
  --resource-group myResourceGroup \
  --name weather-producer \
  --image eventhub-producer:latest \
  --restart-policy Always \
  --environment-variables \
    EVENTHUB_NAME=weather-events \
    BATCH_SIZE=50 \
  --secure-environment-variables \
    EVENTHUB_CONNECTION_STRING="your-connection-string" \
  --memory 1 \
  --cpu 0.5
```

## Monitoring and Observability

### Logging
- Application logs written to `eventhub_producer.log`
- Structured logging with timestamps and log levels
- Performance statistics logged every 10 batches

### Key Metrics to Monitor
```
- Messages sent per second
- Batch success rate
- Error rate and types
- Connection health
- Memory and CPU usage
```

### Azure Monitor Integration
Add Application Insights for comprehensive monitoring:

```python
from applicationinsights import TelemetryClient

tc = TelemetryClient('your-instrumentation-key')
tc.track_metric('messages_sent', self.stats['messages_sent'])
tc.track_metric('error_rate', error_rate)
```

## Performance Tuning

### Throughput Optimization
1. **Increase Batch Size**: Higher batches reduce overhead
2. **Reduce Send Interval**: More frequent sends for real-time needs
3. **Scale Horizontally**: Run multiple producer instances
4. **Partition Strategy**: Use partition keys for even distribution

### Resource Usage
```
Memory: ~100-200MB per instance
CPU: ~10-20% single core utilization
Network: Depends on batch size and frequency
```

### Recommended Production Settings
```bash
# High throughput configuration
BATCH_SIZE=200
SEND_INTERVAL_SECONDS=0.5
MAX_RETRIES=5

# Multiple instances for redundancy
replicas: 3
```

## Security Best Practices

### 1. Connection String Security
- Use Azure Key Vault for connection string storage
- Implement Managed Identity where possible
- Rotate access keys regularly

### 2. Network Security
```bash
# Firewall rules
Allow outbound: *.servicebus.windows.net:5671 (AMQP)
Allow outbound: *.servicebus.windows.net:443 (HTTPS)
```

### 3. Application Security
- Run containers with non-root user
- Implement resource limits
- Use read-only file systems where possible

## Troubleshooting

### Common Issues

#### Connection Errors
```
Error: "The messaging entity could not be found"
Solution: Verify Event Hub name and connection string
```

#### Authentication Failures
```
Error: "Unauthorized access"
Solution: Check access policy permissions (Send permission required)
```

#### Performance Issues
```
Symptom: Low throughput
Solutions:
- Increase BATCH_SIZE
- Decrease SEND_INTERVAL_SECONDS
- Check Event Hub throughput units
- Monitor partition distribution
```

### Health Checks
```bash
# Check producer logs
tail -f eventhub_producer.log

# Monitor resource usage
docker stats weather-producer

# Verify Event Hub metrics in Azure Portal
```

## Scaling Considerations

### Horizontal Scaling
- Run multiple producer instances
- Each instance should use different partition keys
- Monitor Event Hub partition utilization

### Vertical Scaling
- Increase batch size for higher throughput
- Monitor memory usage with larger batches
- Adjust CPU resources based on processing requirements

## Cost Optimization

### Event Hub Costs
- Use auto-scale for throughput units
- Monitor partition usage efficiency
- Consider Basic tier for development/testing

### Compute Costs
- Right-size container resources
- Use spot instances where appropriate
- Implement efficient batching strategies