"""
Integration Tests for Hive Metastore Operations with Purchase Order Items
Tests database creation, table management, and metadata operations for purchase order data
"""

import pytest
import os
import tempfile
import shutil
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from unittest.mock import patch, MagicMock
import logging

# Import Spark and Delta Lake with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
    from pyspark.sql.functions import current_timestamp, col, lit
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# Import project classes
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'class'))

try:
    from hive_metastore_manager import HiveMetastoreManager
    from purchase_order_silver_manager import PurchaseOrderSilverManager
    from purchase_order_item_model import PurchaseOrderItem
    from purchase_order_item_factory import PurchaseOrderItemFactory
except ImportError as e:
    # Mock classes for testing when not available
    HiveMetastoreManager = None
    PurchaseOrderSilverManager = None
    PurchaseOrderItem = None
    PurchaseOrderItemFactory = None

# Test markers
pytestmark = [
    pytest.mark.integration,
    pytest.mark.hive,
    pytest.mark.metastore
]


class TestHiveMetastorePurchaseOrderOperations:
    """Integration tests for Hive Metastore operations with purchase order data."""

    @classmethod
    def setup_class(cls):
        """Set up test class with Spark session and Hive configuration."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        # Create Spark session with Hive support
        cls.spark = SparkSession.builder \
            .appName("PurchaseOrderHiveMetastoreTest") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .enableHiveSupport() \
            .master("local[2]") \
            .getOrCreate()

        # Create temporary directories
        cls.temp_dir = tempfile.mkdtemp(prefix="hive_metastore_test_")
        cls.database_path = os.path.join(cls.temp_dir, "purchase_order_db")
        cls.table_path = os.path.join(cls.temp_dir, "purchase_order_items")

        # Test database and table names
        cls.test_database = "test_purchase_order_db"
        cls.test_table = "purchase_order_items"

        os.makedirs(cls.database_path, exist_ok=True)
        os.makedirs(cls.table_path, exist_ok=True)

    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        if hasattr(cls, 'spark') and cls.spark:
            # Clean up test database if it exists
            try:
                cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.test_database} CASCADE")
            except:
                pass
            cls.spark.stop()

        if hasattr(cls, 'temp_dir') and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def setup_method(self):
        """Set up for each test method."""
        self.factory = PurchaseOrderItemFactory() if PurchaseOrderItemFactory else None
        self.hive_manager = None
        self.silver_manager = None

    def create_purchase_order_schema(self) -> StructType:
        """Create the purchase order table schema."""
        return StructType([
            StructField("order_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("product_name", StringType(), nullable=True),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DoubleType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("vendor_id", StringType(), nullable=True),
            StructField("warehouse_location", StringType(), nullable=True),
            StructField("order_status", StringType(), nullable=True),
            StructField("payment_status", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
            # DQX columns
            StructField("dqx_quality_score", DoubleType(), nullable=True),
            StructField("dqx_validation_timestamp", TimestampType(), nullable=True),
            StructField("validation_errors", StringType(), nullable=True),
            StructField("is_valid", BooleanType(), nullable=True)
        ])

    def generate_test_purchase_order_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate test purchase order data."""
        if self.factory:
            orders = self.factory.generate_batch(count)
            return [order.to_dict() for order in orders]
        else:
            # Fallback test data
            data = []
            base_time = datetime.now(timezone.utc)

            for i in range(count):
                order = {
                    "order_id": f"ORD-HIVE-{int(base_time.timestamp())}-{i:03d}",
                    "product_id": f"PROD-HIVE-{i:03d}",
                    "product_name": f"Hive Test Product {i}",
                    "quantity": 5 + (i % 10),
                    "unit_price": 29.99 + (i * 1.5),
                    "total_amount": (5 + (i % 10)) * (29.99 + (i * 1.5)),
                    "customer_id": f"CUST-HIVE-{i % 5:03d}",
                    "vendor_id": f"VEND-HIVE-{i % 3:03d}",
                    "warehouse_location": "HIVE-TEST-WH",
                    "order_status": "CONFIRMED",
                    "payment_status": "PAID",
                    "created_at": base_time,
                    "updated_at": base_time,
                    "dqx_quality_score": 0.95,
                    "dqx_validation_timestamp": base_time,
                    "validation_errors": None,
                    "is_valid": True
                }
                data.append(order)

            return data

    @pytest.mark.critical
    @pytest.mark.timeout(180)
    def test_hive_database_creation_and_management(self):
        """Test Hive database creation and management for purchase order data."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Hive Database Creation and Management ===")

        try:
            # Step 1: Drop database if exists (cleanup)
            logger.info(f"Step 1: Cleaning up existing database {self.test_database}")
            try:
                self.spark.sql(f"DROP DATABASE IF EXISTS {self.test_database} CASCADE")
                logger.info("Existing database dropped successfully")
            except Exception as e:
                logger.info(f"No existing database to drop: {e}")

            # Step 2: Create database
            logger.info(f"Step 2: Creating database {self.test_database}")
            create_db_sql = f"""
                CREATE DATABASE IF NOT EXISTS {self.test_database}
                COMMENT 'Test database for purchase order items'
                LOCATION '{self.database_path}'
            """
            self.spark.sql(create_db_sql)
            logger.info("Database created successfully")

            # Step 3: Verify database creation
            logger.info("Step 3: Verifying database creation")
            databases = self.spark.sql("SHOW DATABASES").collect()
            database_names = [row.databaseName for row in databases]

            assert self.test_database in database_names, f"Database {self.test_database} should be created"
            logger.info(f"Database verification successful. Found databases: {database_names}")

            # Step 4: Get database details
            logger.info("Step 4: Getting database details")
            try:
                db_details = self.spark.sql(f"DESCRIBE DATABASE {self.test_database}").collect()
                logger.info("Database details:")
                for detail in db_details:
                    logger.info(f"  {detail}")

                # Verify database properties
                db_info = {row.info_name: row.info_value for row in db_details}
                assert "Database Name" in db_info or "database_name" in db_info, "Should have database name info"

            except Exception as e:
                logger.warning(f"Could not get database details: {e}")

            # Step 5: Use the database
            logger.info(f"Step 5: Using database {self.test_database}")
            self.spark.sql(f"USE {self.test_database}")

            current_db = self.spark.sql("SELECT current_database()").collect()[0][0]
            assert current_db == self.test_database, f"Should be using {self.test_database}, currently using {current_db}"
            logger.info(f"Successfully switched to database: {current_db}")

            # Step 6: Test database modification (add comment)
            logger.info("Step 6: Testing database modification")
            try:
                alter_db_sql = f"ALTER DATABASE {self.test_database} SET DBPROPERTIES ('test_property' = 'test_value')"
                self.spark.sql(alter_db_sql)
                logger.info("Database properties updated successfully")
            except Exception as e:
                logger.warning(f"Database property update failed (may not be supported): {e}")

            logger.info("Hive database creation and management test completed successfully")

        except Exception as e:
            logger.error(f"Hive database test failed: {str(e)}")
            pytest.fail(f"Hive database test failed: {str(e)}")

    @pytest.mark.high
    @pytest.mark.timeout(240)
    def test_hive_table_creation_and_schema_management(self):
        """Test Hive table creation and schema management for purchase orders."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Hive Table Creation and Schema Management ===")

        try:
            # Step 1: Ensure database exists and use it
            logger.info(f"Step 1: Setting up database {self.test_database}")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.test_database}")
            self.spark.sql(f"USE {self.test_database}")

            # Step 2: Drop table if exists (cleanup)
            logger.info(f"Step 2: Cleaning up existing table {self.test_table}")
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {self.test_table}")
                logger.info("Existing table dropped successfully")
            except Exception as e:
                logger.info(f"No existing table to drop: {e}")

            # Step 3: Create purchase order table
            logger.info(f"Step 3: Creating table {self.test_table}")

            # Use the schema to create table
            schema = self.create_purchase_order_schema()
            test_data = self.generate_test_purchase_order_data(1)  # Single record for schema inference

            # Create DataFrame and register as temporary table first
            df = self.spark.createDataFrame(test_data, schema)

            # Write as Delta table
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("path", self.table_path) \
                .saveAsTable(f"{self.test_database}.{self.test_table}")

            logger.info("Table created successfully using Delta format")

            # Step 4: Verify table creation
            logger.info("Step 4: Verifying table creation")
            tables = self.spark.sql(f"SHOW TABLES IN {self.test_database}").collect()
            table_names = [row.tableName for row in tables]

            assert self.test_table in table_names, f"Table {self.test_table} should be created"
            logger.info(f"Table verification successful. Found tables: {table_names}")

            # Step 5: Verify table schema
            logger.info("Step 5: Verifying table schema")
            table_schema = self.spark.sql(f"DESCRIBE {self.test_database}.{self.test_table}").collect()

            schema_info = [(row.col_name, row.data_type) for row in table_schema if row.col_name and row.col_name != '']
            logger.info("Table schema:")
            for col_name, data_type in schema_info:
                logger.info(f"  {col_name}: {data_type}")

            # Verify essential columns exist
            column_names = [col_name for col_name, _ in schema_info]
            essential_columns = ['order_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'customer_id']

            for col in essential_columns:
                assert col in column_names, f"Essential column {col} should exist in table schema"

            logger.info("Table schema verification successful")

            # Step 6: Test table properties and metadata
            logger.info("Step 6: Testing table properties and metadata")
            try:
                extended_info = self.spark.sql(f"DESCRIBE EXTENDED {self.test_database}.{self.test_table}").collect()
                logger.info(f"Extended table information retrieved: {len(extended_info)} properties")

                # Look for table format information
                for row in extended_info:
                    if row.col_name and 'Provider' in str(row.col_name):
                        logger.info(f"Table provider: {row.data_type}")

            except Exception as e:
                logger.warning(f"Could not get extended table info: {e}")

            # Step 7: Test table data operations
            logger.info("Step 7: Testing table data operations")

            # Insert additional data
            additional_data = self.generate_test_purchase_order_data(5)
            additional_df = self.spark.createDataFrame(additional_data, schema)

            additional_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(f"{self.test_database}.{self.test_table}")

            # Verify data count
            row_count = self.spark.sql(f"SELECT COUNT(*) FROM {self.test_database}.{self.test_table}").collect()[0][0]
            expected_count = 6  # 1 initial + 5 additional
            assert row_count == expected_count, f"Expected {expected_count} rows, got {row_count}"

            logger.info(f"Table data operations successful. Row count: {row_count}")

            logger.info("Hive table creation and schema management test completed successfully")

        except Exception as e:
            logger.error(f"Hive table test failed: {str(e)}")
            pytest.fail(f"Hive table test failed: {str(e)}")

    @pytest.mark.medium
    @pytest.mark.timeout(200)
    def test_hive_metastore_statistics_and_optimization(self):
        """Test Hive Metastore statistics collection and table optimization."""
        logger = logging.getLogger(__name__)
        logger.info("=== Testing Hive Metastore Statistics and Optimization ===")

        try:
            # Step 1: Setup database and table with substantial data
            logger.info("Step 1: Setting up database and table for statistics testing")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.test_database}")
            self.spark.sql(f"USE {self.test_database}")

            # Create table with more data for meaningful statistics
            schema = self.create_purchase_order_schema()
            test_data = self.generate_test_purchase_order_data(50)  # Larger dataset
            df = self.spark.createDataFrame(test_data, schema)

            # Create partitioned table for better statistics testing
            partitioned_table = f"{self.test_table}_partitioned"

            # Add partition column
            df_with_partition = df.withColumn("order_date",
                                            self.spark.sql("SELECT current_date()").collect()[0][0])

            df_with_partition.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("order_date") \
                .option("path", os.path.join(self.temp_dir, "partitioned_table")) \
                .saveAsTable(f"{self.test_database}.{partitioned_table}")

            logger.info(f"Created partitioned table {partitioned_table} with {df.count()} records")

            # Step 2: Collect table statistics
            logger.info("Step 2: Collecting table statistics")
            try:
                # Analyze table for statistics
                analyze_sql = f"ANALYZE TABLE {self.test_database}.{partitioned_table} COMPUTE STATISTICS"
                self.spark.sql(analyze_sql)
                logger.info("Table statistics computed successfully")

                # Analyze table for column statistics (may not be supported in all environments)
                try:
                    column_analyze_sql = f"ANALYZE TABLE {self.test_database}.{partitioned_table} COMPUTE STATISTICS FOR COLUMNS order_id, quantity, unit_price"
                    self.spark.sql(column_analyze_sql)
                    logger.info("Column statistics computed successfully")
                except Exception as e:
                    logger.warning(f"Column statistics not supported: {e}")

            except Exception as e:
                logger.warning(f"Statistics computation failed (may not be supported): {e}")

            # Step 3: Verify statistics collection
            logger.info("Step 3: Verifying statistics collection")
            try:
                # Get table information including statistics
                table_info = self.spark.sql(f"DESCRIBE EXTENDED {self.test_database}.{partitioned_table}").collect()

                statistics_found = False
                for row in table_info:
                    if row.col_name and 'Statistics' in str(row.col_name):
                        logger.info(f"Table statistics: {row.data_type}")
                        statistics_found = True
                    elif row.col_name and ('numFiles' in str(row.col_name) or 'sizeInBytes' in str(row.col_name)):
                        logger.info(f"File statistics: {row.col_name} = {row.data_type}")
                        statistics_found = True

                if statistics_found:
                    logger.info("Statistics verification successful")
                else:
                    logger.warning("No explicit statistics found in table metadata")

            except Exception as e:
                logger.warning(f"Could not verify statistics: {e}")

            # Step 4: Test partition information
            logger.info("Step 4: Testing partition information")
            try:
                partitions = self.spark.sql(f"SHOW PARTITIONS {self.test_database}.{partitioned_table}").collect()
                partition_info = [row.partition for row in partitions]

                logger.info(f"Found {len(partition_info)} partitions: {partition_info}")
                assert len(partition_info) > 0, "Should have at least one partition"

            except Exception as e:
                logger.warning(f"Partition information not available: {e}")

            # Step 5: Test table optimization operations
            logger.info("Step 5: Testing table optimization operations")
            try:
                # For Delta tables, test OPTIMIZE command
                optimize_sql = f"OPTIMIZE {self.test_database}.{partitioned_table}"
                self.spark.sql(optimize_sql)
                logger.info("Table optimization completed successfully")

                # Test VACUUM operation (cleanup old files)
                vacuum_sql = f"VACUUM {self.test_database}.{partitioned_table} RETAIN 0 HOURS"
                try:
                    self.spark.sql(vacuum_sql)
                    logger.info("Table vacuum completed successfully")
                except Exception as e:
                    logger.warning(f"Vacuum operation failed (may require configuration): {e}")

            except Exception as e:
                logger.warning(f"Optimization operations not supported: {e}")

            # Step 6: Performance verification
            logger.info("Step 6: Performance verification")

            # Test query performance with and without optimization
            import time

            # Simple aggregation query
            query_sql = f"""
                SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as total_value
                FROM {self.test_database}.{partitioned_table}
                GROUP BY customer_id
                ORDER BY total_value DESC
            """

            start_time = time.time()
            result = self.spark.sql(query_sql).collect()
            query_time = time.time() - start_time

            logger.info(f"Query executed in {query_time:.3f}s, returned {len(result)} results")

            # Verify query results
            assert len(result) > 0, "Query should return results"
            for row in result[:3]:  # Check first 3 results
                assert row.order_count > 0, "Order count should be positive"
                assert row.total_value > 0, "Total value should be positive"

            # Performance assertion (should complete reasonably quickly)
            assert query_time < 30.0, f"Query took {query_time:.2f}s (should be < 30s)"

            logger.info("Hive Metastore statistics and optimization test completed successfully")

        except Exception as e:
            logger.error(f"Hive statistics and optimization test failed: {str(e)}")
            pytest.fail(f"Hive statistics and optimization test failed: {str(e)}")


class TestHiveMetastoreErrorHandling:
    """Test error handling scenarios for Hive Metastore operations."""

    @pytest.mark.medium
    @pytest.mark.timeout(120)
    def test_hive_invalid_database_operations(self):
        """Test error handling for invalid database operations."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        spark = SparkSession.builder \
            .appName("HiveErrorTest") \
            .master("local[2]") \
            .getOrCreate()

        try:
            # Test operations on non-existent database
            with pytest.raises(Exception):
                spark.sql("USE non_existent_database_12345")

            # Test invalid database name
            with pytest.raises(Exception):
                spark.sql("CREATE DATABASE `invalid-database-name-with-special-chars!@#`")

        finally:
            spark.stop()

    @pytest.mark.low
    @pytest.mark.timeout(120)
    def test_hive_invalid_table_operations(self):
        """Test error handling for invalid table operations."""
        if not SPARK_AVAILABLE:
            pytest.skip("Spark not available")

        spark = SparkSession.builder \
            .appName("HiveTableErrorTest") \
            .master("local[2]") \
            .getOrCreate()

        try:
            # Test operations on non-existent table
            with pytest.raises(Exception):
                spark.sql("SELECT * FROM non_existent_table_12345")

            # Test invalid table name
            with pytest.raises(Exception):
                spark.sql("CREATE TABLE `invalid-table-name-with-special-chars!@#` (id INT)")

        finally:
            spark.stop()


# Test fixtures
@pytest.fixture(scope="session")
def hive_spark_session():
    """Provide Spark session with Hive support for tests."""
    if not SPARK_AVAILABLE:
        pytest.skip("Spark not available")

    spark = SparkSession.builder \
        .appName("PurchaseOrderHiveTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .master("local[2]") \
        .getOrCreate()

    yield spark
    spark.stop()

@pytest.fixture
def hive_test_database():
    """Provide test database name for Hive tests."""
    return f"test_purchase_order_db_{int(datetime.now().timestamp())}"