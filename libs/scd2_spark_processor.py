#!/usr/bin/env python3
"""
Spark SCD2 Processor with full CDC support (INSERT, UPDATE, DELETE)
Properly handles all operations with before/after data
"""
import sys
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lead, when, lit, md5, concat_ws,
    coalesce, max as spark_max, current_timestamp
)
from pyspark.sql.types import TimestampType
from datetime import datetime
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MINIO_ENDPOINT = 'http://localhost:9002'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'your_minio_secret_here'
BUCKET = 'streaming'
STAGING_PATH = 's3a://streaming/staging/sales_cdc/'

# Nessie Configuration
NESSIE_URI = 'http://localhost:19120/api/v2'
NESSIE_REF = 'main'
WAREHOUSE_PATH = 's3a://lakehouse/iceberg-warehouse/'

# Table Configuration
CATALOG_NAME = 'lakehouse'
DATABASE_NAME = 'default'
TABLE_NAME = 'source_sales_scd2_iceberg'
CHECKPOINT_TABLE = 'source_sales_scd2_checkpoint'


class SparkSCD2Processor:
    def __init__(self):
        self.spark = None
        self.s3_client = None

    def get_s3_client(self):
        """Get S3 client for MinIO"""
        if not self.s3_client:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
        return self.s3_client

    def create_spark_session(self):
        """Create Spark session with Iceberg and Nessie support"""
        logger.info("Creating Spark session...")

        self.spark = (
            SparkSession.builder
            .appName("CDC_SCD2")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
            .config("spark.hadoop.fs.s3a.access.key", "admin")
            .config("spark.hadoop.fs.s3a.secret.key", "your_minio_secret_here")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9002")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

            # Connection and timeout configurations (fixed "60s" error)
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
            .config("spark.hadoop.fs.s3a.connection.maximum", "5000")
            .config("spark.hadoop.fs.s3a.threads.max", "50")
            .config("spark.hadoop.fs.s3a.threads.core", "25")
            .config("spark.hadoop.fs.s3a.max.total.tasks", "50")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
            .config("spark.hadoop.fs.s3a.retry.limit", "10")

            # Multipart upload configurations (fixes "24h" error)
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
            .config("spark.hadoop.fs.s3a.multipart.threshold", "104857600")  # 100MB
            .config("spark.hadoop.fs.s3a.multipart.purge", "false")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24h in milliseconds

            # Additional S3A configurations
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB
            .config("spark.hadoop.fs.s3a.buffer.dir", "/data/spark-tmp/s3a")

            .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.lakehouse.uri", "http://localhost:19121/api/v2")
            .config("spark.sql.catalog.lakehouse.ref", "main")
            .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
            # .config("fs.s3a.experimental.input.fadvise", "sequential")
            .config("spark.jars", ",".join([
                "/opt/spark/jars-extra2/iceberg-spark-runtime-4.0_2.13-1.10.0.jar",
                "/opt/spark/jars-extra2/iceberg-nessie-1.10.0.jar",
                "/opt/spark/jars-extra2/hadoop-aws-3.3.6.jar",
                "/opt/spark/jars-extra2/hadoop-common-3.3.6.jar",
                "/opt/spark/jars-extra2/aws-java-sdk-bundle-1.12.586.jar",
                # "/opt/spark/jars-extra2/iceberg-aws-1.10.0.jar",
            ]))
            .getOrCreate()
        )

        self.spark.sql(f"USE {CATALOG_NAME}")
        logger.info("Spark session created")

    def create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        logger.info(f"Creating database {DATABASE_NAME} if not exists...")

        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DATABASE_NAME}")
            logger.info(f"Database {DATABASE_NAME} ready")
        except Exception as e:
            logger.warning(f"Database creation warning: {e}")

    def create_iceberg_table_if_not_exists(self):
        """Create Iceberg SCD2 table if it doesn't exist"""
        full_table_name = f"{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Creating table {full_table_name} if not exists...")

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id INT,
                product_name STRING,
                category STRING,
                price DOUBLE,
                quantity INT,
                sale_date INT,
                created_at BIGINT,
                effective_start_ts TIMESTAMP,
                effective_end_ts TIMESTAMP,
                is_current BOOLEAN,
                record_hash STRING,
                is_deleted BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(effective_start_ts))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.merge.mode' = 'merge-on-read'
            )
        """

        self.spark.sql(create_table_sql)
        logger.info(f"Table {full_table_name} ready")

    def create_checkpoint_table_if_not_exists(self):
        """Create checkpoint table to track processed data"""
        checkpoint_table = f"{DATABASE_NAME}.{CHECKPOINT_TABLE}"
        logger.info(f"Creating checkpoint table {checkpoint_table}...")

        create_checkpoint_sql = f"""
            CREATE TABLE IF NOT EXISTS {checkpoint_table} (
                last_processed_timestamp TIMESTAMP,
                last_processed_lsn BIGINT,
                processed_at TIMESTAMP,
                records_processed BIGINT
            )
            USING iceberg
        """

        self.spark.sql(create_checkpoint_sql)
        logger.info("Checkpoint table ready")

    def get_last_processed_timestamp(self):
        """Get the last processed timestamp from checkpoint"""
        checkpoint_table = f"{DATABASE_NAME}.{CHECKPOINT_TABLE}"

        try:
            result = self.spark.sql(f"""
                SELECT MAX(last_processed_timestamp) as last_ts
                FROM {checkpoint_table}
            """).collect()

            last_ts = result[0]['last_ts'] if result and result[0]['last_ts'] else None

            if last_ts:
                logger.info(f"Last processed timestamp: {last_ts}")
            else:
                logger.info("No previous checkpoint found, processing all data")

            return last_ts

        except Exception as e:
            logger.warning(f"Could not read checkpoint: {e}")
            return None

    def read_staging_data(self, last_processed_ts=None):
        """Read CDC staging data from MinIO (incremental) with before/after columns"""
        logger.info(f"Reading staging data from {STAGING_PATH}...")

        try:
            staging_df = self.spark.read.format("parquet").load(STAGING_PATH)

            # Apply incremental filter if checkpoint exists
            if last_processed_ts:
                logger.info(f"Filtering for records after {last_processed_ts}")
                staging_df = staging_df.filter(col("event_timestamp") > last_processed_ts)

            record_count = staging_df.count()
            logger.info(f"Read {record_count} records from staging")

            if record_count > 0:
                logger.info("Operation breakdown:")
                staging_df.groupBy("op").count().show()
                staging_df.show(5, truncate=False)

            return staging_df

        except Exception as e:
            logger.error(f"Error reading staging data: {e}")
            raise

    def process_scd2(self, staging_df):
        """
        Apply SCD Type 2 logic with DELETE support

        Operations:
        - INSERT (c, r): New record -> is_current=true, is_deleted=false
        - UPDATE (u): Close old (is_current=false, is_deleted=false), insert new (is_current=true, is_deleted=false)
        - DELETE (d): Close current (is_current=false, is_deleted=true)
        """
        logger.info("Processing SCD Type 2 transformation (including DELETEs)...")

        # Separate deletes from inserts/updates
        deletes_df = staging_df.filter(col("op") == "d")
        inserts_updates_df = staging_df.filter(col("op").isin(["c", "r", "u"]))

        logger.info(f"Operations: {inserts_updates_df.count()} inserts/updates, {deletes_df.count()} deletes")

        # Process INSERT/UPDATE operations
        scd2_inserts_updates = self._process_inserts_updates(inserts_updates_df)

        # Process DELETE operations
        scd2_deletes = self._process_deletes(deletes_df)

        # Union both
        if scd2_inserts_updates is not None and scd2_deletes is not None:
            scd2_df = scd2_inserts_updates.union(scd2_deletes)
        elif scd2_inserts_updates is not None:
            scd2_df = scd2_inserts_updates
        elif scd2_deletes is not None:
            scd2_df = scd2_deletes
        else:
            return None

        if scd2_df.count() > 0:
            logger.info("SCD2 transformation results:")
            scd2_df.show(20, truncate=False)

        total_records = scd2_df.count()
        current_records = scd2_df.filter(col("is_current") == True).count()
        deleted_records = scd2_df.filter(col("is_deleted") == True).count()

        logger.info(f"SCD2 Statistics:")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Current records: {current_records}")
        logger.info(f"Deleted records: {deleted_records}")
        logger.info(f"Historical records: {total_records - current_records - deleted_records}")

        return scd2_df

    def _process_inserts_updates(self, df):
        """
        Process INSERT and UPDATE operations using after_ columns

        Logic:
        - Use after_ columns for new data
        - Detect actual changes using hash comparison
        - Create SCD2 records with proper effective dates
        """
        if df.count() == 0:
            return None

        logger.info("Processing INSERT/UPDATE operations...")

        # Use after_ columns for inserts/updates
        df = df.select(
            col("after_id").alias("id"),
            col("after_product_name").alias("product_name"),
            col("after_category").alias("category"),
            col("after_price").alias("price"),
            col("after_quantity").alias("quantity"),
            col("after_sale_date").alias("sale_date"),
            col("after_created_at").alias("created_at"),
            col("event_timestamp"),
            col("op")
        )

        # Calculate record hash for change detection
        df = df.withColumn(
            "record_hash",
            md5(concat_ws("|",
                          coalesce(col("product_name"), lit("")),
                          coalesce(col("category"), lit("")),
                          coalesce(col("price"), lit("")),
                          coalesce(col("quantity").cast("string"), lit(""))
                          ))
        )

        # Convert price to double
        df = df.withColumn("price", col("price").cast("double"))

        logger.info(f"Records after type conversion: {df.count()}")

        # Window for ordering by ID and timestamp
        window_spec = Window.partitionBy("id").orderBy("event_timestamp")

        # Get next record's hash and timestamp for each row
        df = df.withColumn(
            "next_hash",
            lead("record_hash", 1).over(window_spec)
        ).withColumn(
            "next_timestamp",
            lead("event_timestamp", 1).over(window_spec)
        )

        # Filter out records where hash didn't change (no actual business data change)
        df = df.withColumn(
            "is_change",
            when(col("next_hash").isNull(), lit(True))  # Last record is always a change
            .when(col("record_hash") != col("next_hash"), lit(True))  # Hash changed
            .otherwise(lit(False))  # No change
        )

        # Keep only actual changes
        scd2_df = df.filter(col("is_change") == True)

        logger.info(f"Records after deduplication: {scd2_df.count()}")

        # Build SCD2 structure
        scd2_df = scd2_df.select(
            col("id"),
            col("product_name"),
            col("category"),
            col("price"),
            col("quantity"),
            col("sale_date"),
            col("created_at"),
            col("event_timestamp").alias("effective_start_ts"),
            # effective_end_ts: use next_timestamp if exists, else NULL (current record)
            when(col("next_hash").isNotNull(), col("next_timestamp"))
            .otherwise(lit(None).cast(TimestampType()))
            .alias("effective_end_ts"),
            # is_current: TRUE if no next record (latest version)
            when(col("next_hash").isNull(), lit(True))
            .otherwise(lit(False))
            .alias("is_current"),
            col("record_hash"),
            lit(False).alias("is_deleted"),  # Not deleted
            lit("INSERT_UPDATE").alias("operation_type")  # For tracking
        )

        return scd2_df

    def _process_deletes(self, df):
        """
        Process DELETE operations using before_ columns

        DELETE logic:
        - Use before_ columns to identify which record was deleted
        - Create a marker that will be used to UPDATE the existing current record
        - Set: is_current=false, is_deleted=true, effective_end_ts=delete_timestamp
        """
        if df.count() == 0:
            return None

        logger.info(f"Processing {df.count()} DELETE operations...")

        # For deletes, use before_ columns (what was deleted)
        delete_records = df.select(
            col("before_id").alias("id"),
            col("before_product_name").alias("product_name"),
            col("before_category").alias("category"),
            col("before_price").cast("double").alias("price"),
            col("before_quantity").alias("quantity"),
            col("before_sale_date").alias("sale_date"),
            col("before_created_at").alias("created_at"),
            col("event_timestamp").alias("effective_start_ts"),  # Start time (not used for match)
            lit(None).cast(TimestampType()).alias("effective_end_ts"),  # Will be set by MERGE
            lit(False).alias("is_current"),  # Will be false after delete
            lit("DELETED").alias("record_hash"),  # Special marker
            lit(True).alias("is_deleted"),  # Deleted flag
            lit("DELETE").alias("operation_type")  # For tracking
        )

        return delete_records

    def merge_into_iceberg(self, scd2_df):
        """
        MERGE new SCD2 records into Iceberg table

        Properly handles all operations:
        1. DELETE: Update existing current record (is_current=false, is_deleted=true, effective_end_ts=delete_timestamp)
        2. UPDATE: Close old record (is_current=false, is_deleted=false), insert new record (is_current=true, is_deleted=false)
        3. INSERT: Insert new record (is_current=true, is_deleted=false)
        """
        full_table_name = f"{DATABASE_NAME}.{TABLE_NAME}"
        logger.info(f"Merging into Iceberg table: {full_table_name}...")

        # Separate deletes from inserts/updates based on operation_type
        deletes = scd2_df.filter(col("operation_type") == "DELETE")
        inserts_updates = scd2_df.filter(col("operation_type") == "INSERT_UPDATE")

        delete_count = deletes.count()
        insert_update_count = inserts_updates.count()

        logger.info(f"Operations to process: {insert_update_count} inserts/updates, {delete_count} deletes")

        # Create temp views
        if delete_count > 0:
            deletes.createOrReplaceTempView("delete_operations")

        if insert_update_count > 0:
            inserts_updates.createOrReplaceTempView("new_scd2_records")

        try:
            # MERGE 1: Handle DELETE operations
            # Update existing current records to close them and mark as deleted
            if delete_count > 0:
                logger.info(f"Processing {delete_count} DELETE operations...")

                delete_merge_sql = f"""
                    MERGE INTO {full_table_name} AS target
                    USING delete_operations AS source
                    ON target.id = source.id 
                       AND target.is_current = true
                    WHEN MATCHED THEN
                        UPDATE SET 
                            effective_end_ts = source.effective_start_ts,
                            is_current = false,
                            is_deleted = true
                """

                self.spark.sql(delete_merge_sql)
                logger.info("DELETE operations completed")

            # MERGE 2: Handle INSERT/UPDATE operations
            # Close old records and insert new ones
            if insert_update_count > 0:
                logger.info(f"Processing {insert_update_count} INSERT/UPDATE operations...")

                # Close old current records that have new versions
                close_old_sql = f"""
                    MERGE INTO {full_table_name} AS target
                    USING (
                        SELECT DISTINCT id, effective_start_ts 
                        FROM new_scd2_records 
                        WHERE is_current = true
                    ) AS source
                    ON target.id = source.id 
                       AND target.is_current = true
                    WHEN MATCHED THEN
                        UPDATE SET 
                            effective_end_ts = source.effective_start_ts,
                            is_current = false,
                            is_deleted = false
                """

                self.spark.sql(close_old_sql)
                logger.info("Closed old current records")

                # Insert all new records
                logger.info("Inserting new records...")

                # Remove operation_type column before inserting
                inserts_updates_clean = inserts_updates.drop("operation_type")
                inserts_updates_clean.writeTo(full_table_name).append()

                logger.info("New records inserted")

        except Exception as e:
            logger.error(f"MERGE failed: {e}", exc_info=True)
            raise

    def update_checkpoint(self, staging_df):
        """Update checkpoint with last processed timestamp"""
        checkpoint_table = f"{DATABASE_NAME}.{CHECKPOINT_TABLE}"

        # Get max timestamp and LSN from processed data
        max_values = staging_df.agg(
            spark_max("event_timestamp").alias("max_ts"),
            spark_max("lsn").alias("max_lsn")
        ).collect()[0]

        if max_values['max_ts']:
            from datetime import datetime

            # Create checkpoint DataFrame with explicit schema
            checkpoint_df = self.spark.createDataFrame(
                [(
                    max_values['max_ts'],
                    max_values['max_lsn'],
                    datetime.now(),  
                    staging_df.count()
                )],
                schema=['last_processed_timestamp', 'last_processed_lsn', 'processed_at', 'records_processed']
            )

            checkpoint_df.writeTo(checkpoint_table).append()
            logger.info(f"Checkpoint updated: {max_values['max_ts']}")

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting SCD2 Processor")

            self.create_spark_session()

            self.create_database_if_not_exists()
            self.create_iceberg_table_if_not_exists()
            self.create_checkpoint_table_if_not_exists()

            last_processed_ts = self.get_last_processed_timestamp()

            # Read staging data (incremental)
            staging_df = self.read_staging_data(last_processed_ts)
             
            # count method is not effective here due to potential large data
            # for example, you could check if the DataFrame is empty by using isEmpty() method 
            # if staging_df.rdd.isEmpty():
            #   logger.warning("No new data in staging, exiting...")
            #  return
            if staging_df.count() == 0:
                logger.warning("No new data in staging, exiting...")
                return

            # Process SCD2 (including deletes)
            scd2_df = self.process_scd2(staging_df) 

            if scd2_df is None or scd2_df.count() == 0:
                logger.warning("No records to write after SCD2 processing")
                return

            # MERGE into Iceberg
            self.merge_into_iceberg(scd2_df)

            # Update checkpoint
            self.update_checkpoint(staging_df)

            logger.info("\n" + "=" * 60)
            logger.info("SCD2 processing completed successfully!")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)

        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session closed")


if __name__ == '__main__':
    processor = SparkSCD2Processor()
    processor.run()
