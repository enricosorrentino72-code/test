







from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")




config = {
    "eventhub": {
        "connection_string": dbutils.secrets.get("eventhub-secrets", "connection-string"),
        "event_hub_name": "events",
        "consumer_group": "$Default"
    },
    "adls": {
        "account_name": dbutils.secrets.get("adls-secrets", "account-name"),
        "container_name": "delta-tables",
        "access_key": dbutils.secrets.get("adls-secrets", "access-key")
    },
    "checkpoint_location": "/tmp/eventhub-checkpoints",
    "delta_table_path": "abfss://delta-tables@{}.dfs.core.windows.net/events".format(
        dbutils.secrets.get("adls-secrets", "account-name")
    )
}

spark.conf.set(
    f"fs.azure.account.key.{config['adls']['account_name']}.dfs.core.windows.net",
    config['adls']['access_key']
)

print("Configuration loaded successfully")




event_schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("eventTime", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("data", StringType(), True),
    StructField("dataVersion", StringType(), True),
    StructField("metadataVersion", StringType(), True)
])

print("Event schema defined")




eventhub_conf = {
    "eventhubs.connectionString": config["eventhub"]["connection_string"],
    "eventhubs.consumerGroup": config["eventhub"]["consumer_group"],
    "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
    "eventhubs.maxEventsPerTrigger": "1000"
}

raw_stream = (
    spark
    .readStream
    .format("eventhubs")
    .options(**eventhub_conf)
    .load()
)

print("Event Hub stream created")




parsed_stream = (
    raw_stream
    .select(
        col("body").cast("string").alias("event_body"),
        col("partition").alias("partition_id"),
        col("offset").alias("event_offset"),
        col("sequenceNumber").alias("sequence_number"),
        col("enqueuedTime").alias("enqueued_time"),
        col("properties").alias("event_properties"),
        current_timestamp().alias("processing_time")
    )
    .select(
        "*",
        from_json(col("event_body"), event_schema).alias("parsed_event")
    )
    .select(
        col("partition_id"),
        col("event_offset"),
        col("sequence_number"),
        col("enqueued_time"),
        col("processing_time"),
        col("event_properties"),
        col("parsed_event.eventId").alias("event_id"),
        col("parsed_event.eventType").alias("event_type"),
        col("parsed_event.eventTime").alias("event_time"),
        col("parsed_event.source").alias("event_source"),
        col("parsed_event.subject").alias("event_subject"),
        col("parsed_event.data").alias("event_data"),
        col("parsed_event.dataVersion").alias("data_version"),
        col("parsed_event.metadataVersion").alias("metadata_version")
    )
    .withColumn(
        "partition_date",
        date_format(coalesce(col("event_time"), col("enqueued_time")), "yyyy-MM-dd")
    )
    .withColumn(
        "partition_hour",
        date_format(coalesce(col("event_time"), col("enqueued_time")), "HH")
    )
)

print("Event data parsing configured")




enriched_stream = (
    parsed_stream
    .withColumn(
        "is_valid_event_id",
        when(
            (col("event_id").isNotNull()) & 
            (col("event_id") != "") & 
            (col("event_id").rlike("^[a-zA-Z0-9-]+$")),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "is_valid_event_type",
        when(
            (col("event_type").isNotNull()) & 
            (col("event_type") != ""),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "is_valid_timestamp",
        when(
            col("event_time").isNotNull() | col("enqueued_time").isNotNull(),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "is_valid",
        col("is_valid_event_id") & 
        col("is_valid_event_type") & 
        col("is_valid_timestamp")
    )
    .withColumn("event_type", upper(trim(col("event_type"))))
    .withColumn("event_source", trim(col("event_source")))
    .withColumn(
        "event_category",
        when(col("event_type").rlike("(?i).*user.*"), "USER")
        .when(col("event_type").rlike("(?i).*system.*"), "SYSTEM")
        .when(col("event_type").rlike("(?i).*error.*"), "ERROR")
        .when(col("event_type").rlike("(?i).*security.*"), "SECURITY")
        .when(col("event_type").rlike("(?i).*audit.*"), "AUDIT")
        .otherwise("OTHER")
    )
    .withColumn(
        "processing_latency_seconds",
        (col("processing_time").cast("long") - col("enqueued_time").cast("long"))
    )
    .withColumn("delta_insert_time", current_timestamp())
)

print("Data validation and enrichment configured")




def create_events_table():
    if not DeltaTable.isDeltaTable(spark, config["delta_table_path"]):
        print("Creating events Delta table...")
        
        empty_df = spark.createDataFrame([], enriched_stream.schema)
        
        (
            empty_df
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("partition_date", "partition_hour")
            .save(config["delta_table_path"])
        )
        
        print(f"Events Delta table created at: {config['delta_table_path']}")
    else:
        print("Events Delta table already exists")

create_events_table()




def write_to_delta(batch_df, batch_id):
    """Process each streaming batch and write to Delta table."""
    print(f"Processing batch {batch_id}")
    
    if batch_df.isEmpty():
        print("Empty batch, skipping")
        return
    
    total_count = batch_df.count()
    valid_count = batch_df.filter(col("is_valid") == True).count()
    invalid_count = total_count - valid_count
    
    print(f"Batch {batch_id}: Total={total_count}, Valid={valid_count}, Invalid={invalid_count}")
    
    if valid_count > 0:
        valid_events = batch_df.filter(col("is_valid") == True)
        
        delta_table = DeltaTable.forPath(spark, config["delta_table_path"])
        
        merge_condition = (
            "target.event_id = source.event_id AND " +
            "target.partition_id = source.partition_id AND " +
            "target.event_offset = source.event_offset"
        )
        
        (
            delta_table.alias("target")
            .merge(valid_events.alias("source"), merge_condition)
            .whenMatchedUpdate(set={
                "delta_insert_time": "source.delta_insert_time",
                "processing_time": "source.processing_time"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        print(f"Wrote {valid_count} valid events to Delta table")
    
    if invalid_count > 0:
        print(f"Found {invalid_count} invalid events (implement dead letter handling)")

streaming_query = (
    enriched_stream
    .writeStream
    .foreachBatch(write_to_delta)
    .outputMode("update")
    .option("checkpointLocation", config["checkpoint_location"])
    .trigger(processingTime="30 seconds")
    .queryName("eventhub-delta-pipeline")
    .start()
)

print("Streaming pipeline started")
print(f"Query ID: {streaming_query.id}")




import time

def monitor_streaming_progress(query, duration_minutes=5):
    """Monitor streaming query progress for specified duration."""
    end_time = time.time() + (duration_minutes * 60)
    
    while time.time() < end_time and query.isActive:
        try:
            progress = query.lastProgress
            
            if progress:
                print(f"Batch {progress.get('batchId', 'N/A')}: "
                      f"Input rows: {progress.get('numInputRows', 0)}, "
                      f"Processing rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec, "
                      f"Batch duration: {progress.get('batchDuration', 0)}ms")
            
            time.sleep(10)  # Check every 10 seconds
            
        except Exception as e:
            print(f"Error monitoring progress: {e}")
            break
    
    print("Monitoring completed")

monitor_streaming_progress(streaming_query, duration_minutes=5)




delta_df = spark.read.format("delta").load(config["delta_table_path"])

print(f"Total records in Delta table: {delta_df.count()}")

print("\nSample records:")
delta_df.select(
    "event_id", "event_type", "event_category", 
    "event_time", "partition_date", "is_valid"
).show(10, truncate=False)

print("\nEvent type distribution:")
delta_df.groupBy("event_type", "event_category").count().orderBy("count", ascending=False).show()

print("\nValidation results:")
delta_df.groupBy("is_valid").count().show()




print("Optimizing Delta table...")
delta_table = DeltaTable.forPath(spark, config["delta_table_path"])
delta_table.optimize().executeCompaction()
print("Table optimization completed")

print("\nTable history:")
delta_table.history(5).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
