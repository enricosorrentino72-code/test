import structlog
import logging
import sys
from typing import Any, Dict, Optional
from datetime import datetime


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the pipeline."""
    
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


class PipelineLogger:
    """Enhanced logger for pipeline operations with correlation tracking."""
    
    def __init__(self, component: str, correlation_id: Optional[str] = None):
        self.component = component
        self.correlation_id = correlation_id or self._generate_correlation_id()
        self.logger = get_logger(component).bind(
            component=component,
            correlation_id=self.correlation_id
        )
    
    def _generate_correlation_id(self) -> str:
        """Generate a unique correlation ID."""
        import uuid
        return str(uuid.uuid4())
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with context."""
        self.logger.error(message, **kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(message, **kwargs)
    
    def bind(self, **kwargs) -> 'PipelineLogger':
        """Create a new logger with additional context."""
        new_logger = PipelineLogger(self.component, self.correlation_id)
        new_logger.logger = self.logger.bind(**kwargs)
        return new_logger
    
    def log_streaming_metrics(self, query_progress: Dict[str, Any]) -> None:
        """Log Spark streaming query metrics."""
        self.info(
            "Streaming query progress",
            batch_id=query_progress.get("batchId"),
            input_rows_per_second=query_progress.get("inputRowsPerSecond"),
            processed_rows_per_second=query_progress.get("processedRowsPerSecond"),
            batch_duration_ms=query_progress.get("batchDuration"),
            num_input_rows=query_progress.get("numInputRows"),
            trigger_execution=query_progress.get("triggerExecution", {})
        )
    
    def log_delta_operation(self, operation: str, table_name: str, **kwargs) -> None:
        """Log Delta Lake operation."""
        self.info(
            f"Delta operation: {operation}",
            operation=operation,
            table_name=table_name,
            **kwargs
        )
    
    def log_validation_results(self, total_events: int, valid_events: int, invalid_events: int) -> None:
        """Log event validation results."""
        validation_rate = (valid_events / total_events * 100) if total_events > 0 else 0
        
        self.info(
            "Event validation completed",
            total_events=total_events,
            valid_events=valid_events,
            invalid_events=invalid_events,
            validation_rate_percent=round(validation_rate, 2)
        )
    
    def log_processing_performance(self, start_time: datetime, end_time: datetime, record_count: int) -> None:
        """Log processing performance metrics."""
        duration_seconds = (end_time - start_time).total_seconds()
        records_per_second = record_count / duration_seconds if duration_seconds > 0 else 0
        
        self.info(
            "Processing performance",
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration_seconds,
            record_count=record_count,
            records_per_second=round(records_per_second, 2)
        )
