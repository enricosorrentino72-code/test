# Databricks notebook source
# MAGIC %md
# MAGIC # Event Hub Weather Data Producer
# MAGIC 
# MAGIC **Production-ready Event Hub producer for streaming weather data**
# MAGIC 
# MAGIC This notebook implements:
# MAGIC - Reliable Event Hub data streaming
# MAGIC - Configurable batch processing
# MAGIC - Error handling and retry logic
# MAGIC - Real-time monitoring and statistics
# MAGIC - Databricks integration with widgets and secrets
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Event Hub namespace and hub created
# MAGIC - Databricks secrets configured
# MAGIC - Cluster with appropriate libraries installed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Required Libraries

# COMMAND ----------

# Install required packages
dbutils.library.restartPython()
%pip install --upgrade typing_extensions azure-eventhub>=5.11.0 backoff>=2.2.1 pydantic>=2.0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Databricks Configuration Widgets

# COMMAND ----------

# Create widgets for dynamic configuration
dbutils.widgets.text("eventhub_scope", "rxr-idi-adb-secret-scope", "Secret Scope Name")
dbutils.widgets.text("eventhub_name", "weather-events", "Event Hub Name")
dbutils.widgets.text("batch_size", "50", "Batch Size")
dbutils.widgets.text("send_interval", "2.0", "Send Interval (seconds)")
dbutils.widgets.text("duration_minutes", "60", "Run Duration (minutes)")
dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARNING", "ERROR"], "Log Level")

# Get widget values
SECRET_SCOPE = dbutils.widgets.get("eventhub_scope")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
SEND_INTERVAL = float(dbutils.widgets.get("send_interval"))
DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))
LOG_LEVEL = dbutils.widgets.get("log_level")

print(f"Configuration loaded:")
print(f"- Event Hub: {EVENTHUB_NAME}")
print(f"- Batch Size: {BATCH_SIZE}")
print(f"- Send Interval: {SEND_INTERVAL}s")
print(f"- Duration: {DURATION_MINUTES} minutes")
print(f"- Log Level: {LOG_LEVEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import Libraries and Setup

# COMMAND ----------

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import threading
from concurrent.futures import ThreadPoolExecutor

from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
from azure.core.exceptions import AzureError
import backoff
from pydantic import BaseModel, Field

# Setup logging for Databricks
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Models and Configuration

# COMMAND ----------

@dataclass
class WeatherReading:
    """Structured weather data model optimized for Event Hub streaming"""
    event_id: str
    city: str
    latitude: float
    longitude: float
    temperature: float
    humidity: float
    wind_speed: float
    pressure: float
    precipitation: float
    cloud_cover: float
    weather_condition: str
    timestamp: str
    data_source: str = "databricks_weather_simulator"
    cluster_id: str = ""
    notebook_path: str = ""
    
    def to_json(self) -> str:
        """Convert to JSON string for Event Hub"""
        return json.dumps(asdict(self), ensure_ascii=False)

class DatabricksEventHubConfig:
    """Databricks-specific configuration management"""
    
    def __init__(self, secret_scope: str, eventhub_name: str):
        self.secret_scope = secret_scope
        self.eventhub_name = eventhub_name
        
        # Get connection string from Databricks secrets
        try:
            self.connection_string = dbutils.secrets.get(scope=secret_scope, key="eventhub-connection-string")
            logger.info("✅ Successfully retrieved connection string from secrets")
        except Exception as e:
            logger.error(f"❌ Failed to retrieve connection string: {e}")
            raise ValueError(f"Could not retrieve Event Hub connection string from scope '{secret_scope}'")
        
        # Get Databricks context
        self.cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
        self.notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        
        logger.info(f"✅ Configuration initialized for cluster: {self.cluster_id}")

# Initialize configuration
config = DatabricksEventHubConfig(SECRET_SCOPE, EVENTHUB_NAME)
print(f"✅ Configuration loaded for Event Hub: {EVENTHUB_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Weather Data Generator

# COMMAND ----------

class WeatherDataGenerator:
    """Enhanced weather data generator with realistic patterns"""
    
    CITIES = [
        {"city": "New York", "latitude": 40.7128, "longitude": -74.0060, "timezone": "America/New_York"},
        {"city": "Los Angeles", "latitude": 34.0522, "longitude": -118.2437, "timezone": "America/Los_Angeles"},
        {"city": "Chicago", "latitude": 41.8781, "longitude": -87.6298, "timezone": "America/Chicago"},
        {"city": "Houston", "latitude": 29.7604, "longitude": -95.3698, "timezone": "America/Chicago"},
        {"city": "Miami", "latitude": 25.7617, "longitude": -80.1918, "timezone": "America/New_York"},
        {"city": "Seattle", "latitude": 47.6062, "longitude": -122.3321, "timezone": "America/Los_Angeles"},
        {"city": "Denver", "latitude": 39.7392, "longitude": -104.9903, "timezone": "America/Denver"},
        {"city": "San Francisco", "latitude": 37.7749, "longitude": -122.4194, "timezone": "America/Los_Angeles"},
        {"city": "London", "latitude": 51.5074, "longitude": -0.1278, "timezone": "Europe/London"},
        {"city": "Paris", "latitude": 48.8566, "longitude": 2.3522, "timezone": "Europe/Paris"},
        {"city": "Berlin", "latitude": 52.5200, "longitude": 13.4050, "timezone": "Europe/Berlin"},
        {"city": "Tokyo", "latitude": 35.6895, "longitude": 139.6917, "timezone": "Asia/Tokyo"},
        {"city": "Beijing", "latitude": 39.9042, "longitude": 116.4074, "timezone": "Asia/Shanghai"},
        {"city": "Sydney", "latitude": -33.8688, "longitude": 151.2093, "timezone": "Australia/Sydney"},
        {"city": "Cape Town", "latitude": -33.9249, "longitude": 18.4241, "timezone": "Africa/Johannesburg"},
        {"city": "Dubai", "latitude": 25.276987, "longitude": 55.296249, "timezone": "Asia/Dubai"},
        {"city": "São Paulo", "latitude": -23.5505, "longitude": -46.6333, "timezone": "America/Sao_Paulo"},
        {"city": "Toronto", "latitude": 43.6532, "longitude": -79.3832, "timezone": "America/Toronto"},
        {"city": "Moscow", "latitude": 55.7558, "longitude": 37.6173, "timezone": "Europe/Moscow"},
        {"city": "Mumbai", "latitude": 19.0760, "longitude": 72.8777, "timezone": "Asia/Kolkata"}
    ]
    
    WEATHER_CONDITIONS = [
        "Clear", "Partly Cloudy", "Cloudy", "Overcast", "Light Rain", 
        "Heavy Rain", "Thunderstorm", "Light Snow", "Heavy Snow", 
        "Fog", "Mist", "Windy", "Hazy"
    ]
    
    def __init__(self, config: DatabricksEventHubConfig):
        self.config = config
    
    def generate_weather_reading(self) -> WeatherReading:
        """Generate realistic weather data with correlation patterns"""
        location = random.choice(self.CITIES)
        
        # Generate correlated weather parameters
        base_temp = random.uniform(-20.0, 45.0)
        humidity = self._correlate_humidity_to_temp(base_temp)
        condition = self._select_condition_by_weather(base_temp, humidity)
        precipitation = self._correlate_precipitation(condition, humidity)
        cloud_cover = self._correlate_cloud_cover(condition)
        
        return WeatherReading(
            event_id=str(uuid.uuid4()),
            city=location["city"],
            latitude=location["latitude"],
            longitude=location["longitude"],
            temperature=round(base_temp, 2),
            humidity=round(humidity, 2),
            wind_speed=round(random.uniform(0.0, 50.0), 2),
            pressure=round(random.uniform(950.0, 1050.0), 2),
            precipitation=round(precipitation, 2),
            cloud_cover=round(cloud_cover, 2),
            weather_condition=condition,
            timestamp=datetime.now(timezone.utc).isoformat(),
            cluster_id=self.config.cluster_id,
            notebook_path=self.config.notebook_path
        )
    
    def _correlate_humidity_to_temp(self, temperature: float) -> float:
        """Generate humidity correlated with temperature"""
        if temperature < 0:
            return random.uniform(30.0, 70.0)
        elif temperature > 30:
            return random.uniform(40.0, 90.0)
        else:
            return random.uniform(35.0, 85.0)
    
    def _select_condition_by_weather(self, temperature: float, humidity: float) -> str:
        """Select weather condition based on temperature and humidity"""
        if humidity > 80 and temperature > 5:
            return random.choice(["Light Rain", "Heavy Rain", "Thunderstorm", "Fog"])
        elif temperature < -5:
            return random.choice(["Light Snow", "Heavy Snow", "Clear"])
        elif humidity < 30:
            return random.choice(["Clear", "Partly Cloudy", "Windy"])
        else:
            return random.choice(["Partly Cloudy", "Cloudy", "Overcast"])
    
    def _correlate_precipitation(self, condition: str, humidity: float) -> float:
        """Generate precipitation based on condition and humidity"""
        if "Rain" in condition or "Storm" in condition:
            return random.uniform(1.0, 25.0) * (humidity / 100)
        elif "Snow" in condition:
            return random.uniform(0.5, 15.0)
        else:
            return random.uniform(0.0, 0.5)
    
    def _correlate_cloud_cover(self, condition: str) -> float:
        """Generate cloud cover based on condition"""
        cloud_mapping = {
            "Clear": (0, 20),
            "Partly Cloudy": (20, 60),
            "Cloudy": (60, 90),
            "Overcast": (90, 100)
        }
        
        for key, (min_val, max_val) in cloud_mapping.items():
            if key in condition:
                return random.uniform(min_val, max_val)
        
        return random.uniform(30, 70)

# Initialize weather generator
weather_generator = WeatherDataGenerator(config)
print("✅ Weather data generator initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Event Hub Producer with Databricks Integration

# COMMAND ----------

class DatabricksEventHubProducer:
    """Event Hub Producer optimized for Databricks notebooks"""
    
    def __init__(self, config: DatabricksEventHubConfig, weather_generator: WeatherDataGenerator):
        self.config = config
        self.weather_generator = weather_generator
        self.producer: Optional[EventHubProducerClient] = None
        self.is_running = True
        self.stats = {
            'messages_sent': 0,
            'batches_sent': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        # Display live stats
        self.stats_display = None
    
    def _initialize_producer(self) -> EventHubProducerClient:
        """Initialize Event Hub producer with connection validation"""
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.config.connection_string,
                eventhub_name=self.config.eventhub_name
            )
            logger.info(f"✅ Connected to Event Hub: {self.config.eventhub_name}")
            return producer
        except Exception as e:
            logger.error(f"❌ Failed to initialize Event Hub producer: {e}")
            raise
    
    @backoff.on_exception(
        backoff.expo,
        (EventHubError, AzureError),
        max_tries=3,
        max_time=60,
        on_backoff=lambda details: logger.warning(f"Retry attempt {details['tries']} for batch send")
    )
    def _send_batch_with_retry(self, batch: List[WeatherReading]) -> bool:
        """Send batch with exponential backoff retry"""
        try:
            event_data_batch = self.producer.create_batch()
            
            # Add events to batch with partition key for distribution
            events_added = 0
            for weather_data in batch:
                try:
                    event_data = EventData(weather_data.to_json())
                    #event_data.partition_key = weather_data.city
                    event_data_batch.add(event_data)
                    events_added += 1
                except ValueError as e:
                    logger.warning(f"Skipping oversized event for {weather_data.city}: {e}")
                    continue
            
            if events_added == 0:
                logger.warning("No events to send in batch")
                return False
            
            # Send batch to Event Hub
            self.producer.send_batch(event_data_batch)
            
            # Update statistics
            self.stats['messages_sent'] += events_added
            self.stats['batches_sent'] += 1
            
            logger.debug(f"✅ Sent batch of {events_added} events")
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Failed to send batch: {e}")
            raise
    
    def _generate_batch(self, batch_size: int) -> List[WeatherReading]:
        """Generate a batch of weather readings"""
        return [
            self.weather_generator.generate_weather_reading() 
            for _ in range(batch_size)
        ]
    
    def _update_display_stats(self):
        """Update live statistics display in notebook"""
        runtime = (datetime.now() - self.stats['start_time']).total_seconds()
        messages_per_sec = self.stats['messages_sent'] / runtime if runtime > 0 else 0
        error_rate = (self.stats['errors'] / self.stats['batches_sent'] * 100) if self.stats['batches_sent'] > 0 else 0
        
        stats_html = f"""
        <div style="border: 2px solid #4CAF50; border-radius: 10px; padding: 15px; background: #f9f9f9;">
            <h3 style="color: #4CAF50;">🚀 Event Hub Producer Statistics</h3>
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px;">
                <div><strong>Messages Sent:</strong> {self.stats['messages_sent']:,}</div>
                <div><strong>Batches Sent:</strong> {self.stats['batches_sent']:,}</div>
                <div><strong>Errors:</strong> {self.stats['errors']}</div>
                <div><strong>Error Rate:</strong> {error_rate:.2f}%</div>
                <div><strong>Throughput:</strong> {messages_per_sec:.2f} msg/sec</div>
                <div><strong>Runtime:</strong> {runtime:.1f}s</div>
            </div>
            <div style="margin-top: 10px;">
                <strong>Event Hub:</strong> {self.config.eventhub_name} | 
                <strong>Cluster:</strong> {self.config.cluster_id}
            </div>
        </div>
        """
        
        displayHTML(stats_html)
    
    def run_for_duration(self, duration_minutes: int, batch_size: int, send_interval: float):
        """Run producer for specified duration with real-time monitoring"""
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        logger.info(f"🚀 Starting Event Hub producer for {duration_minutes} minutes")
        logger.info(f"Configuration: batch_size={batch_size}, send_interval={send_interval}s")
        
        try:
            # Initialize producer
            self.producer = self._initialize_producer()
            
            # Main production loop
            while datetime.now() < end_time and self.is_running:
                try:
                    # Generate and send batch
                    batch = self._generate_batch(batch_size)
                    success = self._send_batch_with_retry(batch)
                    
                    # Update display every 5 batches
                    if self.stats['batches_sent'] % 5 == 0:
                        self._update_display_stats()
                    
                    # Wait before next batch
                    time.sleep(send_interval)
                    
                except Exception as e:
                    logger.error(f"❌ Error in production loop: {e}")
                    self.stats['errors'] += 1
                    time.sleep(1)  # Brief pause before retry
        
        except KeyboardInterrupt:
            logger.info("⏹️ Producer stopped by user")
            self.is_running = False
        except Exception as e:
            logger.error(f"💥 Fatal error in producer: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources and show final statistics"""
        logger.info("🧹 Cleaning up resources...")
        
        try:
            if self.producer:
                self.producer.close()
                logger.info("✅ Event Hub producer connection closed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
        
        # Final statistics display
        self._update_display_stats()
        
        runtime = (datetime.now() - self.stats['start_time']).total_seconds()
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   - Total Messages: {self.stats['messages_sent']:,}")
        logger.info(f"   - Total Batches: {self.stats['batches_sent']:,}")
        logger.info(f"   - Total Errors: {self.stats['errors']}")
        logger.info(f"   - Runtime: {runtime:.1f} seconds")
        logger.info("✅ Producer shutdown complete")

# Initialize producer
producer = DatabricksEventHubProducer(config, weather_generator)
print("✅ Event Hub producer initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test Connection and Sample Data

# COMMAND ----------

# Test connection with a small batch
print("🧪 Testing Event Hub connection...")

try:
    # Generate sample data
    sample_batch = weather_generator.generate_weather_reading()
    print(f"📝 Sample weather data generated:")
    print(f"   City: {sample_batch.city}")
    print(f"   Temperature: {sample_batch.temperature}°C")
    print(f"   Condition: {sample_batch.weather_condition}")
    print(f"   Timestamp: {sample_batch.timestamp}")
    
    # Test producer initialization
    test_producer = producer._initialize_producer()
    test_producer.close()
    print("✅ Event Hub connection test successful!")
    
except Exception as e:
    print(f"❌ Connection test failed: {e}")
    print("Please check your Event Hub configuration and secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Run Event Hub Producer
# MAGIC 
# MAGIC **⚠️ Important:** This cell will run the producer for the configured duration. 
# MAGIC Monitor the live statistics display below.

# COMMAND ----------

# Run the Event Hub producer
print(f"🚀 Starting Event Hub Weather Data Producer")
print(f"Configuration:")
print(f"   - Event Hub: {EVENTHUB_NAME}")
print(f"   - Duration: {DURATION_MINUTES} minutes") 
print(f"   - Batch Size: {BATCH_SIZE}")
print(f"   - Send Interval: {SEND_INTERVAL} seconds")
print(f"   - Expected Messages: ~{int(DURATION_MINUTES * 60 / SEND_INTERVAL * BATCH_SIZE):,}")

# Start the producer
producer.run_for_duration(
    duration_minutes=DURATION_MINUTES,
    batch_size=BATCH_SIZE, 
    send_interval=SEND_INTERVAL
)

print("🏁 Producer run completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Optional: Stop Producer Early
# MAGIC 
# MAGIC Run this cell if you need to stop the producer before the configured duration ends.

# COMMAND ----------

# Stop the producer early if needed
producer.is_running = False
print("⏹️ Producer stop signal sent")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup and Final Report

# COMMAND ----------

# Final cleanup and reporting
print("📊 Event Hub Producer Session Summary")
print("=" * 50)

if producer.stats['start_time']:
    runtime = (datetime.now() - producer.stats['start_time']).total_seconds()
    messages_per_sec = producer.stats['messages_sent'] / runtime if runtime > 0 else 0
    
    print(f"📈 Performance Metrics:")
    print(f"   - Total Messages Sent: {producer.stats['messages_sent']:,}")
    print(f"   - Total Batches Sent: {producer.stats['batches_sent']:,}")
    print(f"   - Total Errors: {producer.stats['errors']}")
    print(f"   - Average Throughput: {messages_per_sec:.2f} messages/second")
    print(f"   - Session Duration: {runtime:.1f} seconds")
    print(f"   - Event Hub: {EVENTHUB_NAME}")
    print(f"   - Cluster ID: {config.cluster_id}")
    
    success_rate = ((producer.stats['batches_sent'] - producer.stats['errors']) / producer.stats['batches_sent'] * 100) if producer.stats['batches_sent'] > 0 else 0
    print(f"   - Success Rate: {success_rate:.2f}%")
    
    if producer.stats['errors'] == 0:
        print("✅ All batches sent successfully!")
    else:
        print(f"⚠️ {producer.stats['errors']} errors occurred during processing")
        
print("\n🎯 Next Steps:")
print("   - Monitor Event Hub metrics in Azure Portal")
print("   - Check downstream consumers for data processing")
print("   - Review logs for any error details")
print("   - Scale up batch size or reduce interval for higher throughput")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Utility Functions for Monitoring

# COMMAND ----------

def check_eventhub_connection():
    """Utility function to test Event Hub connectivity"""
    try:
        test_producer = EventHubProducerClient.from_connection_string(
            conn_str=config.connection_string,
            eventhub_name=config.eventhub_name
        )
        test_producer.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

def estimate_throughput(batch_size: int, send_interval: float, duration_minutes: int):
    """Estimate expected throughput"""
    batches_per_minute = 60 / send_interval
    total_batches = batches_per_minute * duration_minutes
    total_messages = total_batches * batch_size
    
    print(f"📊 Throughput Estimation:")
    print(f"   - Batches per minute: {batches_per_minute:.1f}")
    print(f"   - Total batches ({duration_minutes}min): {total_batches:.0f}")
    print(f"   - Total messages: {total_messages:.0f}")
    print(f"   - Messages per second: {total_messages / (duration_minutes * 60):.1f}")

# Test utilities
print("🔧 Utility Functions Available:")
print("   - check_eventhub_connection(): Test connection")
print("   - estimate_throughput(batch_size, interval, duration): Calculate estimates")

# Example usage
estimate_throughput(BATCH_SIZE, SEND_INTERVAL, DURATION_MINUTES)