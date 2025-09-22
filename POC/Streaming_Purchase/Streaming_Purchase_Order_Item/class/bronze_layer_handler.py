# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Handler
# MAGIC
# MAGIC Handler class for managing Bronze layer operations in ADLS Gen2.
# MAGIC
# MAGIC ## Features:
# MAGIC - ADLS Gen2 mounting and configuration
# MAGIC - Partition management by date and hour
# MAGIC - Data validation and quality checks
# MAGIC - Storage optimization
# MAGIC - Path management

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, min as spark_min, max as spark_max
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import os

# COMMAND ----------

class BronzeLayerHandler:
    """
    Handler class for Bronze layer operations in ADLS Gen2.

    This class manages:
    - ADLS Gen2 storage configuration
    - Data partitioning strategies
    - Path management
    - Storage optimization
    - Data validation
    """

    def __init__(self,
                 spark: SparkSession,
                 storage_account: str,
                 container: str,
                 bronze_path: str,
                 log_level: str = "INFO"):
        """
        Initialize the Bronze Layer Handler.

        Args:
            spark: Active Spark session
            storage_account: ADLS Gen2 storage account name
            container: Container name
            bronze_path: Base path for Bronze layer data
            log_level: Logging level
        """
        self.spark = spark
        self.storage_account = storage_account
        self.container = container
        self.bronze_path = bronze_path

        # Set up logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(getattr(logging, log_level))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Initialize ADLS configuration
        self._configure_adls()

    def _configure_adls(self):
        """Configure ADLS Gen2 connection settings."""
        try:
            self.logger.info(f"Configuring ADLS Gen2 connection to {self.storage_account}")

            # Set ADLS Gen2 configuration
            self.spark.conf.set(
                f"fs.azure.account.auth.type.{self.storage_account}.dfs.core.windows.net",
                "SharedKey"
            )

            # Note: In production, use Key Vault or managed identity
            # For now, assuming the key is available in environment or configured elsewhere

            self.logger.info("ADLS Gen2 configuration completed")

        except Exception as e:
            self.logger.error(f"Failed to configure ADLS Gen2: {e}")
            raise

    def get_full_path(self, relative_path: str = "") -> str:
        """
        Get the full ADLS Gen2 path.

        Args:
            relative_path: Optional relative path to append

        Returns:
            Full ADLS Gen2 path
        """
        base_path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net{self.bronze_path}"
        if relative_path:
            return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"
        return base_path

    def get_partition_path(self, ingestion_date: str, ingestion_hour: int) -> str:
        """
        Get the partition path for a specific date and hour.

        Args:
            ingestion_date: Date in YYYY-MM-DD format
            ingestion_hour: Hour (0-23)

        Returns:
            Full partition path
        """
        partition_path = f"ingestion_date={ingestion_date}/ingestion_hour={ingestion_hour:02d}"
        return self.get_full_path(partition_path)

    def create_directory_structure(self):
        """Create the base directory structure for Bronze layer."""
        try:
            self.logger.info("Creating Bronze layer directory structure")

            base_path = self.get_full_path()

            # Create a dummy DataFrame to trigger directory creation
            dummy_df = self.spark.range(1).select(col("id").alias("dummy"))

            # Write and immediately delete to create directory
            temp_path = f"{base_path}/temp_structure"
            dummy_df.write.mode("overwrite").parquet(temp_path)

            # Clean up temporary files
            dbutils.fs.rm(temp_path, True)

            self.logger.info(f"Directory structure created at: {base_path}")

        except Exception as e:
            self.logger.error(f"Failed to create directory structure: {e}")
            raise

    def write_batch_to_bronze(self,
                             batch_df: DataFrame,
                             write_mode: str = "append",
                             partition_columns: List[str] = None) -> Dict[str, Any]:
        """
        Write a batch DataFrame to Bronze layer with partitioning.

        Args:
            batch_df: DataFrame to write
            write_mode: Write mode (append, overwrite, etc.)
            partition_columns: Columns to partition by

        Returns:
            Dictionary with write operation results
        """
        if partition_columns is None:
            partition_columns = ["ingestion_date", "ingestion_hour"]

        try:
            output_path = self.get_full_path()
            record_count = batch_df.count()

            self.logger.info(f"Writing {record_count} records to Bronze layer")
            self.logger.debug(f"Output path: {output_path}")

            # Write with partitioning
            (
                batch_df
                .write
                .mode(write_mode)
                .partitionBy(*partition_columns)
                .option("compression", "snappy")
                .parquet(output_path)
            )

            # Calculate partition information
            partition_info = self._get_partition_info(batch_df, partition_columns)

            result = {
                "records_written": record_count,
                "output_path": output_path,
                "partitions_created": partition_info,
                "write_mode": write_mode,
                "timestamp": datetime.utcnow().isoformat()
            }

            self.logger.info(f"Successfully wrote {record_count} records to Bronze layer")
            return result

        except Exception as e:
            self.logger.error(f"Failed to write batch to Bronze layer: {e}")
            raise

    def _get_partition_info(self, df: DataFrame, partition_columns: List[str]) -> List[Dict[str, Any]]:
        """
        Get partition information from DataFrame.

        Args:
            df: DataFrame to analyze
            partition_columns: Partition columns

        Returns:
            List of partition information
        """
        try:
            if len(partition_columns) == 2 and "ingestion_date" in partition_columns and "ingestion_hour" in partition_columns:
                partitions = (
                    df
                    .groupBy("ingestion_date", "ingestion_hour")
                    .agg(count("*").alias("record_count"))
                    .orderBy("ingestion_date", "ingestion_hour")
                    .collect()
                )

                return [
                    {
                        "ingestion_date": row.ingestion_date,
                        "ingestion_hour": row.ingestion_hour,
                        "record_count": row.record_count
                    }
                    for row in partitions
                ]
            else:
                return []

        except Exception as e:
            self.logger.warning(f"Could not get partition info: {e}")
            return []

    def read_bronze_data(self,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        hours: Optional[List[int]] = None) -> DataFrame:
        """
        Read data from Bronze layer with optional filtering.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            hours: List of hours to include (0-23)

        Returns:
            DataFrame with Bronze layer data
        """
        try:
            base_path = self.get_full_path()
            self.logger.info(f"Reading Bronze layer data from: {base_path}")

            # Read all parquet files
            df = self.spark.read.parquet(base_path)

            # Apply date filters if specified
            if start_date:
                df = df.filter(col("ingestion_date") >= start_date)
                self.logger.debug(f"Applied start date filter: {start_date}")

            if end_date:
                df = df.filter(col("ingestion_date") <= end_date)
                self.logger.debug(f"Applied end date filter: {end_date}")

            if hours:
                df = df.filter(col("ingestion_hour").isin(hours))
                self.logger.debug(f"Applied hour filter: {hours}")

            record_count = df.count()
            self.logger.info(f"Read {record_count} records from Bronze layer")

            return df

        except Exception as e:
            self.logger.error(f"Failed to read Bronze layer data: {e}")
            raise

    def get_storage_statistics(self) -> Dict[str, Any]:
        """
        Get storage statistics for Bronze layer.

        Returns:
            Dictionary with storage statistics
        """
        try:
            base_path = self.get_full_path()

            # Get file system information
            files_info = dbutils.fs.ls(base_path)

            total_size = 0
            file_count = 0

            def calculate_size(path_info):
                nonlocal total_size, file_count
                for item in path_info:
                    if item.isFile():
                        total_size += item.size
                        file_count += 1
                    elif item.isDir():
                        try:
                            sub_items = dbutils.fs.ls(item.path)
                            calculate_size(sub_items)
                        except:
                            pass  # Skip inaccessible directories

            calculate_size(files_info)

            # Try to get record count
            try:
                df = self.spark.read.parquet(base_path)
                record_count = df.count()
            except:
                record_count = 0

            # Get partition information
            partitions = self._get_partition_list()

            stats = {
                "base_path": base_path,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "file_count": file_count,
                "record_count": record_count,
                "partition_count": len(partitions),
                "partitions": partitions[:10],  # First 10 partitions
                "collection_time": datetime.utcnow().isoformat()
            }

            self.logger.info(f"Storage statistics collected: {stats['total_size_mb']} MB, {stats['record_count']} records")
            return stats

        except Exception as e:
            self.logger.error(f"Failed to get storage statistics: {e}")
            return {"error": str(e)}

    def _get_partition_list(self) -> List[str]:
        """
        Get list of existing partitions.

        Returns:
            List of partition paths
        """
        try:
            base_path = self.get_full_path()
            partitions = []

            # List date partitions
            date_partitions = dbutils.fs.ls(base_path)
            for date_partition in date_partitions:
                if date_partition.isDir() and "ingestion_date=" in date_partition.name:
                    # List hour partitions within each date
                    hour_partitions = dbutils.fs.ls(date_partition.path)
                    for hour_partition in hour_partitions:
                        if hour_partition.isDir() and "ingestion_hour=" in hour_partition.name:
                            partitions.append(f"{date_partition.name}/{hour_partition.name}")

            return sorted(partitions)

        except Exception as e:
            self.logger.warning(f"Could not list partitions: {e}")
            return []

    def cleanup_old_partitions(self, retention_days: int = 30) -> Dict[str, Any]:
        """
        Clean up old partitions beyond retention period.

        Args:
            retention_days: Number of days to retain

        Returns:
            Dictionary with cleanup results
        """
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
            base_path = self.get_full_path()

            self.logger.info(f"Starting cleanup of partitions older than {cutoff_date}")

            deleted_partitions = []
            total_size_deleted = 0

            # List date partitions
            date_partitions = dbutils.fs.ls(base_path)
            for date_partition in date_partitions:
                if date_partition.isDir() and "ingestion_date=" in date_partition.name:
                    # Extract date from partition name
                    partition_date = date_partition.name.replace("ingestion_date=", "")

                    if partition_date < cutoff_date:
                        # Calculate size before deletion
                        partition_size = self._calculate_partition_size(date_partition.path)

                        # Delete the partition
                        dbutils.fs.rm(date_partition.path, True)

                        deleted_partitions.append({
                            "partition": date_partition.name,
                            "size_mb": round(partition_size / (1024 * 1024), 2)
                        })
                        total_size_deleted += partition_size

            result = {
                "cutoff_date": cutoff_date,
                "partitions_deleted": len(deleted_partitions),
                "total_size_deleted_mb": round(total_size_deleted / (1024 * 1024), 2),
                "deleted_partitions": deleted_partitions,
                "cleanup_time": datetime.utcnow().isoformat()
            }

            self.logger.info(f"Cleanup completed: {len(deleted_partitions)} partitions deleted, "
                           f"{result['total_size_deleted_mb']} MB freed")

            return result

        except Exception as e:
            self.logger.error(f"Failed to cleanup old partitions: {e}")
            return {"error": str(e)}

    def _calculate_partition_size(self, partition_path: str) -> int:
        """
        Calculate the total size of a partition.

        Args:
            partition_path: Path to the partition

        Returns:
            Total size in bytes
        """
        try:
            total_size = 0
            items = dbutils.fs.ls(partition_path)

            for item in items:
                if item.isFile():
                    total_size += item.size
                elif item.isDir():
                    total_size += self._calculate_partition_size(item.path)

            return total_size

        except Exception:
            return 0

    def validate_bronze_data(self, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Validate Bronze layer data quality.

        Args:
            sample_size: Number of records to sample for validation

        Returns:
            Dictionary with validation results
        """
        try:
            self.logger.info(f"Starting Bronze layer data validation (sample size: {sample_size})")

            base_path = self.get_full_path()
            df = self.spark.read.parquet(base_path)

            # Basic statistics
            total_records = df.count()
            sample_df = df.sample(False, min(1.0, sample_size / total_records)) if total_records > 0 else df

            # Validation checks
            validation_results = {
                "total_records": total_records,
                "sample_size": min(sample_size, total_records),
                "validation_time": datetime.utcnow().isoformat(),
                "checks": {}
            }

            if total_records > 0:
                # Check for null record_ids
                null_record_ids = sample_df.filter(col("record_id").isNull()).count()
                validation_results["checks"]["null_record_ids"] = null_record_ids

                # Check for invalid JSON in raw_payload
                try:
                    invalid_json = sample_df.filter(col("raw_payload").isNull() | (col("raw_payload") == "")).count()
                    validation_results["checks"]["invalid_json_payloads"] = invalid_json
                except:
                    validation_results["checks"]["invalid_json_payloads"] = "N/A"

                # Check data quality distribution
                try:
                    quality_distribution = (
                        sample_df
                        .groupBy("is_valid")
                        .agg(count("*").alias("count"))
                        .collect()
                    )
                    validation_results["checks"]["quality_distribution"] = {
                        row.is_valid: row.count for row in quality_distribution
                    }
                except:
                    validation_results["checks"]["quality_distribution"] = "N/A"

                # Check partition distribution
                try:
                    partition_distribution = (
                        sample_df
                        .groupBy("ingestion_date")
                        .agg(count("*").alias("count"))
                        .orderBy("ingestion_date")
                        .collect()
                    )
                    validation_results["checks"]["partition_distribution"] = {
                        row.ingestion_date: row.count for row in partition_distribution
                    }
                except:
                    validation_results["checks"]["partition_distribution"] = "N/A"

            self.logger.info("Bronze layer data validation completed")
            return validation_results

        except Exception as e:
            self.logger.error(f"Failed to validate Bronze layer data: {e}")
            return {"error": str(e)}

# COMMAND ----------

# Export for use in other notebooks
__all__ = ['BronzeLayerHandler']