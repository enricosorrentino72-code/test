# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Manager
# MAGIC
# MAGIC Manager class for creating and managing Hive Metastore tables for the Bronze layer.
# MAGIC
# MAGIC ## Features:
# MAGIC - Automatic table creation with proper schema
# MAGIC - Partition management and repair
# MAGIC - Table statistics and optimization
# MAGIC - Schema evolution support
# MAGIC - Access control management

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, DateType, DoubleType
)
import logging
from typing import Dict, List, Any, Optional

# COMMAND ----------

class HiveMetastoreManager:
    """
    Manager class for Hive Metastore operations for Bronze layer tables.

    This class handles:
    - Table creation and schema management
    - Partition operations
    - Table maintenance and optimization
    - Statistics collection
    - Access control
    """

    def __init__(self,
                 spark: SparkSession,
                 database_name: str,
                 table_name: str,
                 bronze_path: str,
                 log_level: str = "INFO"):
        """
        Initialize the Hive Metastore Manager.

        Args:
            spark: Active Spark session
            database_name: Target database name
            table_name: Target table name
            bronze_path: Path to Bronze layer data
            log_level: Logging level
        """
        self.spark = spark
        self.database_name = database_name
        self.table_name = table_name
        self.bronze_path = bronze_path
        self.full_table_name = f"{database_name}.{table_name}"

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Define table schema
        self._define_table_schema()

    def _define_table_schema(self):
        """Define the schema for the Bronze layer table."""
        self.table_schema = StructType([
            # Technical metadata
            StructField("record_id", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", DateType(), False),
            StructField("ingestion_hour", IntegerType(), False),

            # EventHub metadata
            StructField("eventhub_partition", IntegerType(), True),
            StructField("eventhub_offset", LongType(), True),
            StructField("eventhub_sequence_number", LongType(), True),
            StructField("eventhub_enqueued_time", TimestampType(), True),

            # Processing metadata
            StructField("processing_cluster_id", StringType(), True),
            StructField("consumer_group", StringType(), True),
            StructField("eventhub_name", StringType(), True),

            # Payload
            StructField("raw_payload", StringType(), False),
            StructField("payload_size_bytes", IntegerType(), True),

            # Parsed business fields
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("is_valid", StringType(), True),
            StructField("data_quality_score", DoubleType(), True)
        ])

    def create_database(self) -> bool:
        """
        Create the database if it doesn't exist.

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Creating database: {self.database_name}")

            create_db_sql = f"""
            CREATE DATABASE IF NOT EXISTS {self.database_name}
            COMMENT 'Bronze layer database for purchase order streaming data'
            """

            self.spark.sql(create_db_sql)
            self.logger.info(f"Database {self.database_name} created successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create database {self.database_name}: {e}")
            return False

    def create_table(self,
                    partition_columns: List[str] = None,
                    table_properties: Dict[str, str] = None) -> bool:
        """
        Create the Bronze layer table with partitioning.

        Args:
            partition_columns: Columns to partition by
            table_properties: Additional table properties

        Returns:
            True if successful, False otherwise
        """
        if partition_columns is None:
            partition_columns = ["ingestion_date", "ingestion_hour"]

        if table_properties is None:
            table_properties = {
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            }

        try:
            self.logger.info(f"Creating table: {self.full_table_name}")

            # First ensure database exists
            self.create_database()

            # Check if table already exists
            if self.table_exists():
                self.logger.info(f"Table {self.full_table_name} already exists")
                return True

            # Build column definitions
            column_defs = []
            for field in self.table_schema.fields:
                nullable = "NULL" if field.nullable else "NOT NULL"
                column_defs.append(f"`{field.name}` {self._spark_to_sql_type(field.dataType)} {nullable}")

            columns_sql = ",\n    ".join(column_defs)

            # Build partition clause
            partition_clause = ""
            if partition_columns:
                partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"

            # Build table properties clause
            properties_clause = ""
            if table_properties:
                props = [f"'{k}' = '{v}'" for k, v in table_properties.items()]
                properties_clause = f"TBLPROPERTIES ({', '.join(props)})"

            # Create table SQL
            create_table_sql = f"""
            CREATE TABLE {self.full_table_name} (
                {columns_sql}
            )
            USING PARQUET
            {partition_clause}
            LOCATION '{self.bronze_path}'
            {properties_clause}
            COMMENT 'Bronze layer table for purchase order items from EventHub'
            """

            self.spark.sql(create_table_sql)
            self.logger.info(f"Table {self.full_table_name} created successfully")

            # Add table comment with metadata
            self._add_table_metadata()

            return True

        except Exception as e:
            self.logger.error(f"Failed to create table {self.full_table_name}: {e}")
            return False

    def _spark_to_sql_type(self, spark_type) -> str:
        """
        Convert Spark data type to SQL type string.

        Args:
            spark_type: Spark data type

        Returns:
            SQL type string
        """
        type_mapping = {
            "StringType": "STRING",
            "IntegerType": "INT",
            "LongType": "BIGINT",
            "DoubleType": "DOUBLE",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE"
        }

        type_name = type(spark_type).__name__
        return type_mapping.get(type_name, "STRING")

    def table_exists(self) -> bool:
        """
        Check if the table exists in the metastore.

        Returns:
            True if table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.full_table_name}")
            return True
        except:
            return False

    def repair_table(self) -> Dict[str, Any]:
        """
        Repair table partitions (equivalent to MSCK REPAIR TABLE).

        Returns:
            Dictionary with repair results
        """
        try:
            self.logger.info(f"Repairing table partitions for {self.full_table_name}")

            # Get partitions before repair
            partitions_before = self._get_partition_count()

            # Run repair command
            repair_sql = f"MSCK REPAIR TABLE {self.full_table_name}"
            self.spark.sql(repair_sql)

            # Get partitions after repair
            partitions_after = self._get_partition_count()

            result = {
                "table_name": self.full_table_name,
                "partitions_before": partitions_before,
                "partitions_after": partitions_after,
                "partitions_added": partitions_after - partitions_before,
                "repair_time": datetime.utcnow().isoformat()
            }

            self.logger.info(f"Table repair completed. Added {result['partitions_added']} partitions")
            return result

        except Exception as e:
            self.logger.error(f"Failed to repair table {self.full_table_name}: {e}")
            return {"error": str(e)}

    def _get_partition_count(self) -> int:
        """
        Get the number of partitions in the table.

        Returns:
            Number of partitions
        """
        try:
            partitions_df = self.spark.sql(f"SHOW PARTITIONS {self.full_table_name}")
            return partitions_df.count()
        except:
            return 0

    def add_partition(self, ingestion_date: str, ingestion_hour: int) -> bool:
        """
        Add a specific partition to the table.

        Args:
            ingestion_date: Date in YYYY-MM-DD format
            ingestion_hour: Hour (0-23)

        Returns:
            True if successful, False otherwise
        """
        try:
            partition_spec = f"ingestion_date='{ingestion_date}', ingestion_hour={ingestion_hour}"
            partition_path = f"{self.bronze_path}/ingestion_date={ingestion_date}/ingestion_hour={ingestion_hour:02d}"

            add_partition_sql = f"""
            ALTER TABLE {self.full_table_name}
            ADD IF NOT EXISTS PARTITION ({partition_spec})
            LOCATION '{partition_path}'
            """

            self.spark.sql(add_partition_sql)
            self.logger.info(f"Added partition: {partition_spec}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to add partition {partition_spec}: {e}")
            return False

    def drop_partition(self, ingestion_date: str, ingestion_hour: int) -> bool:
        """
        Drop a specific partition from the table.

        Args:
            ingestion_date: Date in YYYY-MM-DD format
            ingestion_hour: Hour (0-23)

        Returns:
            True if successful, False otherwise
        """
        try:
            partition_spec = f"ingestion_date='{ingestion_date}', ingestion_hour={ingestion_hour}"

            drop_partition_sql = f"""
            ALTER TABLE {self.full_table_name}
            DROP IF EXISTS PARTITION ({partition_spec})
            """

            self.spark.sql(drop_partition_sql)
            self.logger.info(f"Dropped partition: {partition_spec}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop partition {partition_spec}: {e}")
            return False

    def update_table_statistics(self) -> Dict[str, Any]:
        """
        Update table statistics for better query performance.

        Returns:
            Dictionary with statistics update results
        """
        try:
            self.logger.info(f"Updating table statistics for {self.full_table_name}")

            # Analyze table for statistics
            analyze_sql = f"ANALYZE TABLE {self.full_table_name} COMPUTE STATISTICS"
            self.spark.sql(analyze_sql)

            # Analyze columns for better optimization
            key_columns = ["order_id", "customer_id", "order_status", "ingestion_date"]
            for column in key_columns:
                try:
                    column_analyze_sql = f"ANALYZE TABLE {self.full_table_name} COMPUTE STATISTICS FOR COLUMNS {column}"
                    self.spark.sql(column_analyze_sql)
                except:
                    pass  # Column might not exist

            # Get updated statistics
            stats = self.get_table_statistics()

            self.logger.info("Table statistics updated successfully")
            return stats

        except Exception as e:
            self.logger.error(f"Failed to update table statistics: {e}")
            return {"error": str(e)}

    def get_table_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive table statistics.

        Returns:
            Dictionary with table statistics
        """
        try:
            stats = {
                "table_name": self.full_table_name,
                "collection_time": datetime.utcnow().isoformat()
            }

            # Basic table info
            try:
                describe_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {self.full_table_name}")
                describe_rows = describe_df.collect()

                for row in describe_rows:
                    if row[0] == "Statistics":
                        stats["file_statistics"] = row[1]
                    elif row[0] == "Location":
                        stats["location"] = row[1]
                    elif row[0] == "Created Time":
                        stats["created_time"] = row[1]
            except:
                pass

            # Record count
            try:
                record_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.full_table_name}").collect()[0].count
                stats["record_count"] = record_count
            except:
                stats["record_count"] = 0

            # Partition count
            stats["partition_count"] = self._get_partition_count()

            # Data quality summary
            try:
                quality_stats = self.spark.sql(f"""
                    SELECT
                        is_valid,
                        COUNT(*) as count,
                        AVG(data_quality_score) as avg_quality_score
                    FROM {self.full_table_name}
                    GROUP BY is_valid
                """).collect()

                stats["quality_summary"] = {
                    row.is_valid: {
                        "count": row.count,
                        "avg_quality_score": round(row.avg_quality_score or 0, 3)
                    }
                    for row in quality_stats
                }
            except:
                stats["quality_summary"] = {}

            # Recent partitions
            try:
                recent_partitions = self.spark.sql(f"""
                    SELECT DISTINCT ingestion_date, ingestion_hour, COUNT(*) as records
                    FROM {self.full_table_name}
                    GROUP BY ingestion_date, ingestion_hour
                    ORDER BY ingestion_date DESC, ingestion_hour DESC
                    LIMIT 10
                """).collect()

                stats["recent_partitions"] = [
                    {
                        "date": row.ingestion_date,
                        "hour": row.ingestion_hour,
                        "records": row.records
                    }
                    for row in recent_partitions
                ]
            except:
                stats["recent_partitions"] = []

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get table statistics: {e}")
            return {"error": str(e)}

    def _add_table_metadata(self):
        """Add metadata comments to the table."""
        try:
            metadata_sql = f"""
            ALTER TABLE {self.full_table_name}
            SET TBLPROPERTIES (
                'created_by' = 'PurchaseOrderItemPipeline',
                'data_source' = 'Azure EventHub',
                'layer' = 'Bronze',
                'partition_columns' = 'ingestion_date,ingestion_hour',
                'schema_version' = '1.0'
            )
            """
            self.spark.sql(metadata_sql)
        except Exception as e:
            self.logger.warning(f"Could not add table metadata: {e}")

    def grant_table_permissions(self, principal: str, permissions: List[str]) -> bool:
        """
        Grant permissions on the table.

        Args:
            principal: User or group to grant permissions to
            permissions: List of permissions (SELECT, INSERT, etc.)

        Returns:
            True if successful, False otherwise
        """
        try:
            for permission in permissions:
                grant_sql = f"GRANT {permission} ON TABLE {self.full_table_name} TO `{principal}`"
                self.spark.sql(grant_sql)

            self.logger.info(f"Granted permissions {permissions} to {principal} on {self.full_table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to grant permissions: {e}")
            return False

    def drop_table(self) -> bool:
        """
        Drop the table (use with caution).

        Returns:
            True if successful, False otherwise
        """
        try:
            drop_sql = f"DROP TABLE IF EXISTS {self.full_table_name}"
            self.spark.sql(drop_sql)
            self.logger.info(f"Dropped table: {self.full_table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to drop table {self.full_table_name}: {e}")
            return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_hive_manager_from_widgets(spark, dbutils) -> HiveMetastoreManager:
    """
    Create a Hive Metastore Manager instance from Databricks widget values.

    Args:
        spark: Spark session
        dbutils: Databricks utilities

    Returns:
        Configured HiveMetastoreManager instance
    """
    # Get widget values
    DATABASE_NAME = dbutils.widgets.get("database_name")
    TABLE_NAME = dbutils.widgets.get("table_name")
    BRONZE_PATH = dbutils.widgets.get("bronze_path")
    LOG_LEVEL = dbutils.widgets.get("log_level")

    # Create and return manager
    return HiveMetastoreManager(
        spark=spark,
        database_name=DATABASE_NAME,
        table_name=TABLE_NAME,
        bronze_path=BRONZE_PATH,
        log_level=LOG_LEVEL
    )

# COMMAND ----------

# Export for use in other notebooks
__all__ = ['HiveMetastoreManager', 'create_hive_manager_from_widgets']