#!/usr/bin/env python3
"""
Main entry point for the Azure Event Hub to Delta Lake pipeline.
"""

import sys
import signal
import time
from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from config.settings import get_settings
from consumer.eventhub_consumer import EventHubConsumer
from processing.data_processor import DataProcessor
from delta.delta_operations import DeltaLakeManager
from monitoring.logger import configure_logging, get_logger, PipelineLogger
from monitoring.metrics import PipelineMetrics, MetricsContext
from monitoring.health_check import HealthChecker, StreamingHealthChecker


class EventHubDeltaPipeline:
    """Main pipeline orchestrator for Event Hub to Delta Lake data ingestion."""
    
    def __init__(self):
        self.settings = get_settings()
        
        configure_logging(self.settings.monitoring.log_level)
        self.logger = PipelineLogger("pipeline_orchestrator")
        
        self.spark: Optional[SparkSession] = None
        self.consumer: Optional[EventHubConsumer] = None
        self.processor: Optional[DataProcessor] = None
        self.delta_manager: Optional[DeltaLakeManager] = None
        self.metrics: Optional[PipelineMetrics] = None
        self.health_checker: Optional[HealthChecker] = None
        self.streaming_health_checker: Optional[StreamingHealthChecker] = None
        
        self.is_running = False
        self.streaming_queries = []
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.stop()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with Delta Lake support."""
        self.logger.info("Creating Spark session")
        
        try:
            builder = (
                SparkSession.builder
                .appName(self.settings.spark.app_name)
                .master(self.settings.spark.master)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.streaming.checkpointLocation", self.settings.eventhub.checkpoint_location)
            )
            
            if self.settings.adls.access_key:
                builder = builder.config(
                    f"spark.hadoop.fs.azure.account.key.{self.settings.adls.account_name}.dfs.core.windows.net",
                    self.settings.adls.access_key
                )
            
            builder = configure_spark_with_delta_pip(builder)
            
            spark = builder.getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(
                "Successfully created Spark session",
                app_id=spark.sparkContext.applicationId,
                app_name=spark.sparkContext.appName
            )
            
            return spark
            
        except Exception as e:
            self.logger.error(
                "Failed to create Spark session",
                error=str(e),
                exc_info=True
            )
            raise
    
    def _initialize_components(self):
        """Initialize all pipeline components."""
        self.logger.info("Initializing pipeline components")
        
        try:
            self.spark = self._create_spark_session()
            
            self.consumer = EventHubConsumer(
                self.spark,
                self.settings.eventhub,
                self.settings.spark
            )
            
            self.processor = DataProcessor(self.spark)
            
            self.delta_manager = DeltaLakeManager(
                self.spark,
                self.settings.adls
            )
            
            self.metrics = PipelineMetrics(
                self.settings.monitoring.metrics_port,
                self.settings.monitoring.enable_metrics
            )
            
            self.health_checker = HealthChecker(self.settings.monitoring.health_check_port)
            self.streaming_health_checker = StreamingHealthChecker(self.health_checker)
            
            self._register_health_checks()
            
            self.logger.info("Successfully initialized all pipeline components")
            
        except Exception as e:
            self.logger.error(
                "Failed to initialize pipeline components",
                error=str(e),
                exc_info=True
            )
            raise
    
    def _register_health_checks(self):
        """Register custom health checks."""
        def check_spark_session():
            return self.spark is not None and not self.spark._jsc.sc().isStopped()
        
        def check_delta_tables():
            try:
                events_table_path = self.delta_manager.get_delta_table_path("events")
                return True  # If we can get the path without error, consider it healthy
            except Exception:
                return False
        
        self.health_checker.register_check("spark_session", check_spark_session)
        self.health_checker.register_check("delta_tables", check_delta_tables)
    
    def _create_streaming_pipeline(self):
        """Create the main streaming pipeline."""
        self.logger.info("Creating streaming pipeline")
        
        try:
            with MetricsContext(self.metrics, "create_pipeline", "orchestrator"):
                raw_stream = self.consumer.create_eventhub_stream()
                
                parsed_stream = self.consumer.parse_event_data(raw_stream)
                
                def process_batch(batch_df, batch_id):
                    """Process each streaming batch."""
                    batch_logger = self.logger.bind(batch_id=batch_id)
                    batch_start_time = datetime.utcnow()
                    
                    try:
                        batch_logger.info("Processing streaming batch", batch_id=batch_id)
                        
                        if batch_df.isEmpty():
                            batch_logger.info("Empty batch, skipping processing")
                            return
                        
                        input_count = batch_df.count()
                        batch_logger.info("Batch input count", count=input_count)
                        
                        valid_events, invalid_events = self.processor.process_events(batch_df)
                        
                        valid_count = valid_events.count()
                        invalid_count = invalid_events.count()
                        
                        batch_logger.info(
                            "Batch processing results",
                            input_count=input_count,
                            valid_count=valid_count,
                            invalid_count=invalid_count
                        )
                        
                        self.metrics.record_event_processed("valid", count=valid_count)
                        self.metrics.record_event_processed("invalid", count=invalid_count)
                        
                        if valid_count > 0:
                            with MetricsContext(self.metrics, "upsert_events", "delta_manager"):
                                self.delta_manager.upsert_events(valid_events, "events")
                            batch_logger.info("Successfully wrote valid events to Delta table")
                        
                        if invalid_count > 0:
                            dead_letter_events = self.processor.create_dead_letter_record(invalid_events)
                            with MetricsContext(self.metrics, "append_dead_letter", "delta_manager"):
                                self.delta_manager.append_dead_letter_events(dead_letter_events)
                            batch_logger.info("Successfully wrote invalid events to dead letter table")
                        
                        batch_end_time = datetime.utcnow()
                        self.logger.log_processing_performance(
                            batch_start_time,
                            batch_end_time,
                            input_count
                        )
                        
                        self.metrics.set_pipeline_health("batch_processing", True)
                        
                    except Exception as e:
                        batch_logger.error(
                            "Failed to process batch",
                            error=str(e),
                            batch_id=batch_id,
                            exc_info=True
                        )
                        
                        self.metrics.record_error("batch_processing", type(e).__name__)
                        self.metrics.set_pipeline_health("batch_processing", False)
                        
                        raise
                
                query = (
                    parsed_stream
                    .writeStream
                    .foreachBatch(process_batch)
                    .outputMode("update")
                    .option("checkpointLocation", self.settings.eventhub.checkpoint_location)
                    .trigger(processingTime=self.settings.spark.trigger_interval)
                    .queryName("eventhub-delta-pipeline")
                )
                
                return query
                
        except Exception as e:
            self.logger.error(
                "Failed to create streaming pipeline",
                error=str(e),
                exc_info=True
            )
            raise
    
    def start(self):
        """Start the pipeline."""
        self.logger.info("Starting Event Hub Delta Pipeline")
        
        try:
            self._initialize_components()
            
            self.health_checker.start_server()
            
            self.delta_manager.create_events_table()
            self.delta_manager.create_dead_letter_table()
            
            streaming_query = self._create_streaming_pipeline()
            query = streaming_query.start()
            
            self.streaming_health_checker.register_streaming_query("main_pipeline", query)
            self.streaming_queries.append(query)
            
            self.is_running = True
            
            self.logger.info(
                "Successfully started pipeline",
                query_id=query.id,
                query_name=query.name
            )
            
            self._monitor_streaming_progress(query)
            
        except Exception as e:
            self.logger.error(
                "Failed to start pipeline",
                error=str(e),
                exc_info=True
            )
            self.stop()
            raise
    
    def _monitor_streaming_progress(self, query):
        """Monitor streaming query progress and log metrics."""
        self.logger.info("Starting streaming progress monitoring")
        
        try:
            while self.is_running and query.isActive:
                query.awaitTermination(30)  # Wait up to 30 seconds
                
                if query.lastProgress:
                    progress = query.lastProgress
                    
                    self.logger.log_streaming_metrics(progress)
                    
                    self.metrics.record_streaming_metrics("main_pipeline", progress)
                    
                    self.metrics.set_pipeline_health("streaming_query", True)
                
                if query.exception:
                    self.logger.error(
                        "Streaming query failed",
                        exception=str(query.exception),
                        query_id=query.id
                    )
                    self.metrics.record_error("streaming_query", "QueryException")
                    self.metrics.set_pipeline_health("streaming_query", False)
                    break
                    
        except Exception as e:
            self.logger.error(
                "Error in streaming progress monitoring",
                error=str(e),
                exc_info=True
            )
    
    def stop(self):
        """Stop the pipeline gracefully."""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Event Hub Delta Pipeline")
        
        try:
            self.is_running = False
            
            for query in self.streaming_queries:
                if query.isActive:
                    self.logger.info("Stopping streaming query", query_id=query.id)
                    query.stop()
            
            if self.health_checker:
                self.health_checker.stop_server()
            
            if self.spark:
                self.spark.stop()
            
            self.logger.info("Successfully stopped pipeline")
            
        except Exception as e:
            self.logger.error(
                "Error during pipeline shutdown",
                error=str(e),
                exc_info=True
            )
    
    def run(self):
        """Run the pipeline (blocking call)."""
        try:
            self.start()
            
            while self.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(
                "Pipeline execution failed",
                error=str(e),
                exc_info=True
            )
        finally:
            self.stop()


def main():
    """Main entry point."""
    pipeline = EventHubDeltaPipeline()
    
    try:
        pipeline.run()
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(
            "Pipeline failed to start",
            error=str(e),
            exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
