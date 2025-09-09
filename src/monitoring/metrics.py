import time
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, start_http_server, CollectorRegistry
from datetime import datetime
import structlog

from .logger import get_logger

logger = get_logger(__name__)


class PipelineMetrics:
    """Prometheus metrics collector for the Event Hub Delta pipeline."""
    
    def __init__(self, metrics_port: int = 8080, enable_metrics: bool = True):
        self.metrics_port = metrics_port
        self.enable_metrics = enable_metrics
        self.registry = CollectorRegistry()
        self.logger = logger.bind(component="metrics")
        
        if self.enable_metrics:
            self._initialize_metrics()
            self._start_metrics_server()
    
    def _initialize_metrics(self) -> None:
        """Initialize Prometheus metrics."""
        self.logger.info("Initializing Prometheus metrics")
        
        self.events_processed_total = Counter(
            'events_processed_total',
            'Total number of events processed',
            ['status', 'event_type', 'source'],
            registry=self.registry
        )
        
        self.events_validation_total = Counter(
            'events_validation_total',
            'Total number of events validated',
            ['validation_result', 'validation_reason'],
            registry=self.registry
        )
        
        self.processing_duration_seconds = Histogram(
            'processing_duration_seconds',
            'Time spent processing events',
            ['operation', 'component'],
            registry=self.registry
        )
        
        self.streaming_batch_size = Histogram(
            'streaming_batch_size',
            'Number of records in streaming batch',
            ['query_name'],
            registry=self.registry
        )
        
        self.streaming_batch_duration_seconds = Histogram(
            'streaming_batch_duration_seconds',
            'Duration of streaming batch processing',
            ['query_name'],
            registry=self.registry
        )
        
        self.streaming_input_rate = Gauge(
            'streaming_input_rate_per_second',
            'Input rate of streaming query',
            ['query_name'],
            registry=self.registry
        )
        
        self.streaming_processing_rate = Gauge(
            'streaming_processing_rate_per_second',
            'Processing rate of streaming query',
            ['query_name'],
            registry=self.registry
        )
        
        self.delta_operations_total = Counter(
            'delta_operations_total',
            'Total number of Delta Lake operations',
            ['operation', 'table_name', 'status'],
            registry=self.registry
        )
        
        self.delta_operation_duration_seconds = Histogram(
            'delta_operation_duration_seconds',
            'Duration of Delta Lake operations',
            ['operation', 'table_name'],
            registry=self.registry
        )
        
        self.delta_table_size_bytes = Gauge(
            'delta_table_size_bytes',
            'Size of Delta table in bytes',
            ['table_name'],
            registry=self.registry
        )
        
        self.errors_total = Counter(
            'errors_total',
            'Total number of errors',
            ['component', 'error_type'],
            registry=self.registry
        )
        
        self.pipeline_health = Gauge(
            'pipeline_health',
            'Pipeline health status (1=healthy, 0=unhealthy)',
            ['component'],
            registry=self.registry
        )
        
        self.last_successful_batch_timestamp = Gauge(
            'last_successful_batch_timestamp',
            'Timestamp of last successful batch processing',
            ['query_name'],
            registry=self.registry
        )
        
        self.logger.info("Successfully initialized Prometheus metrics")
    
    def _start_metrics_server(self) -> None:
        """Start Prometheus metrics HTTP server."""
        try:
            start_http_server(self.metrics_port, registry=self.registry)
            self.logger.info(
                "Started Prometheus metrics server",
                port=self.metrics_port
            )
        except Exception as e:
            self.logger.error(
                "Failed to start metrics server",
                error=str(e),
                port=self.metrics_port
            )
            raise
    
    def record_event_processed(self, status: str, event_type: str = "unknown", source: str = "unknown") -> None:
        """Record an event processing metric."""
        if self.enable_metrics:
            self.events_processed_total.labels(
                status=status,
                event_type=event_type,
                source=source
            ).inc()
    
    def record_validation_result(self, is_valid: bool, reason: str = "unknown") -> None:
        """Record event validation result."""
        if self.enable_metrics:
            result = "valid" if is_valid else "invalid"
            self.events_validation_total.labels(
                validation_result=result,
                validation_reason=reason
            ).inc()
    
    def record_processing_duration(self, operation: str, component: str, duration_seconds: float) -> None:
        """Record processing duration."""
        if self.enable_metrics:
            self.processing_duration_seconds.labels(
                operation=operation,
                component=component
            ).observe(duration_seconds)
    
    def record_streaming_metrics(self, query_name: str, progress: Dict[str, Any]) -> None:
        """Record streaming query metrics."""
        if not self.enable_metrics:
            return
        
        try:
            num_input_rows = progress.get("numInputRows", 0)
            self.streaming_batch_size.labels(query_name=query_name).observe(num_input_rows)
            
            batch_duration = progress.get("batchDuration", 0)
            if batch_duration:
                self.streaming_batch_duration_seconds.labels(query_name=query_name).observe(batch_duration / 1000.0)
            
            input_rate = progress.get("inputRowsPerSecond", 0)
            if input_rate is not None:
                self.streaming_input_rate.labels(query_name=query_name).set(input_rate)
            
            processing_rate = progress.get("processedRowsPerSecond", 0)
            if processing_rate is not None:
                self.streaming_processing_rate.labels(query_name=query_name).set(processing_rate)
            
            self.last_successful_batch_timestamp.labels(query_name=query_name).set(time.time())
            
        except Exception as e:
            self.logger.error(
                "Failed to record streaming metrics",
                error=str(e),
                query_name=query_name
            )
    
    def record_delta_operation(self, operation: str, table_name: str, duration_seconds: float, status: str = "success") -> None:
        """Record Delta Lake operation metrics."""
        if self.enable_metrics:
            self.delta_operations_total.labels(
                operation=operation,
                table_name=table_name,
                status=status
            ).inc()
            
            self.delta_operation_duration_seconds.labels(
                operation=operation,
                table_name=table_name
            ).observe(duration_seconds)
    
    def record_error(self, component: str, error_type: str) -> None:
        """Record error metric."""
        if self.enable_metrics:
            self.errors_total.labels(
                component=component,
                error_type=error_type
            ).inc()
    
    def set_pipeline_health(self, component: str, is_healthy: bool) -> None:
        """Set pipeline health status."""
        if self.enable_metrics:
            health_value = 1.0 if is_healthy else 0.0
            self.pipeline_health.labels(component=component).set(health_value)
    
    def set_table_size(self, table_name: str, size_bytes: int) -> None:
        """Set Delta table size metric."""
        if self.enable_metrics:
            self.delta_table_size_bytes.labels(table_name=table_name).set(size_bytes)


class MetricsContext:
    """Context manager for timing operations and recording metrics."""
    
    def __init__(self, metrics: PipelineMetrics, operation: str, component: str):
        self.metrics = metrics
        self.operation = operation
        self.component = component
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.metrics.record_processing_duration(
                self.operation,
                self.component,
                duration
            )
            
            if exc_type:
                self.metrics.record_error(
                    self.component,
                    exc_type.__name__
                )
