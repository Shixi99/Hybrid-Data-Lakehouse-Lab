#!/usr/bin/env python3
"""
CDC to Parquet Streaming Pipeline
Captures both 'before' and 'after' data for proper DELETE handling
"""
import os
import sys
import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class CDCToParquetPipeline:
    def __init__(self):
        self.table_env = None
        self.flink_home = "/home/your_username/pipeline/flink-1.18.1"
        
        # Configuration
        self.kafka_servers = 'localhost:9092'
        self.kafka_topic = 'mydb.public.source_sales'
        self.minio_endpoint = 'http://localhost:9002'
        self.minio_access_key = 'admin'
        self.minio_secret_key = 'your_minio_secret_here'
        self.bucket = 'streaming'
    
    def create_environment(self):
        """Create Flink Table Environment"""
        logger.info("Creating Flink environment...")
        
        os.environ['FLINK_HOME'] = self.flink_home
        
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.table_env = TableEnvironment.create(env_settings)
        
        # Required JARs
        jars = [
            f"{self.flink_home}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
            f"{self.flink_home}/lib/flink-json-1.18.1.jar",
            f"{self.flink_home}/lib/flink-sql-parquet-1.18.1.jar",
            f"{self.flink_home}/lib/hadoop-aws-3.3.4.jar",
            f"{self.flink_home}/lib/aws-java-sdk-bundle-1.12.367.jar"
        ]
        
        # Validate JARs
        missing = [j for j in jars if not os.path.exists(j)]
        if missing:
            raise FileNotFoundError(f"Missing JARs: {missing}")
        
        # Add JARs
        for jar in jars:
            self.table_env.get_config().set("pipeline.jars", f"file://{jar}")
        
        # Configuration
        config = self.table_env.get_config()
        config.set("pipeline.name", "CDC-to-Parquet-Streaming")
        config.set("execution.checkpointing.interval", "60s")
        config.set("parallelism.default", "1")
        
        # MinIO configuration
        config.set("s3.endpoint", self.minio_endpoint)
        config.set("s3.access-key", self.minio_access_key)
        config.set("s3.secret-key", self.minio_secret_key)
        config.set("s3.path.style.access", "true")
        
        logger.info("Environment created")
    
    def create_cdc_source_table(self):
        """Create Kafka source table - captures BOTH before and after"""
        logger.info("Creating Kafka CDC source table...")
        
        # Read as raw string
        self.table_env.execute_sql("""
            CREATE TABLE source_sales_cdc_raw (
                raw_message STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '""" + self.kafka_topic + """',
                'properties.bootstrap.servers' = '""" + self.kafka_servers + """',
                'properties.group.id' = 'flink-cdc-to-parquet',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'raw'
            )
        """)
        
        # Create view to parse BOTH before and after
        self.table_env.execute_sql("""
            CREATE TEMPORARY VIEW source_sales_cdc AS
            SELECT 
                -- AFTER data (for INSERT/UPDATE)
                CAST(JSON_VALUE(raw_message, '$.payload.after.id') AS INT) as after_id,
                JSON_VALUE(raw_message, '$.payload.after.product_name') as after_product_name,
                JSON_VALUE(raw_message, '$.payload.after.category') as after_category,
                JSON_VALUE(raw_message, '$.payload.after.price') as after_price,
                CAST(JSON_VALUE(raw_message, '$.payload.after.quantity') AS INT) as after_quantity,
                CAST(JSON_VALUE(raw_message, '$.payload.after.sale_date') AS INT) as after_sale_date,
                CAST(JSON_VALUE(raw_message, '$.payload.after.created_at') AS BIGINT) as after_created_at,
                
                -- BEFORE data (for DELETE)
                CAST(JSON_VALUE(raw_message, '$.payload.before.id') AS INT) as before_id,
                JSON_VALUE(raw_message, '$.payload.before.product_name') as before_product_name,
                JSON_VALUE(raw_message, '$.payload.before.category') as before_category,
                JSON_VALUE(raw_message, '$.payload.before.price') as before_price,
                CAST(JSON_VALUE(raw_message, '$.payload.before.quantity') AS INT) as before_quantity,
                CAST(JSON_VALUE(raw_message, '$.payload.before.sale_date') AS INT) as before_sale_date,
                CAST(JSON_VALUE(raw_message, '$.payload.before.created_at') AS BIGINT) as before_created_at,
                
                -- Operation type
                JSON_VALUE(raw_message, '$.payload.op') as op,
                CAST(JSON_VALUE(raw_message, '$.payload.ts_ms') AS BIGINT) as ts_ms,
                JSON_VALUE(raw_message, '$.payload.source.db') as source_db,
                JSON_VALUE(raw_message, '$.payload.source.table') as source_table,
                CAST(JSON_VALUE(raw_message, '$.payload.source.txId') AS BIGINT) as txId,
                CAST(JSON_VALUE(raw_message, '$.payload.source.lsn') AS BIGINT) as lsn,
                TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(raw_message, '$.payload.ts_ms') AS BIGINT) / 1000)) as event_time
            FROM source_sales_cdc_raw
            WHERE JSON_VALUE(raw_message, '$.payload.op') IS NOT NULL
        """)
        
        logger.info("CDC source table created (with before and after)")
    
    def create_parquet_sink_table(self):
        """Create MinIO Parquet sink table"""
        logger.info("Creating Parquet sink table...")
        
        self.table_env.execute_sql(f"""
            CREATE TABLE sales_cdc_staging (
                -- After data
                after_id INT,
                after_product_name STRING,
                after_category STRING,
                after_price STRING,
                after_quantity INT,
                after_sale_date INT,
                after_created_at BIGINT,
                
                -- Before data
                before_id INT,
                before_product_name STRING,
                before_category STRING,
                before_price STRING,
                before_quantity INT,
                before_sale_date INT,
                before_created_at BIGINT,
                
                -- Metadata
                event_timestamp TIMESTAMP(3),
                op STRING,
                source_db STRING,
                source_table STRING,
                txId BIGINT,
                lsn BIGINT,
                dt STRING
            ) PARTITIONED BY (dt) WITH (
                'connector' = 'filesystem',
                'path' = 's3a://{self.bucket}/staging/sales_cdc/',
                'format' = 'parquet',
                'sink.partition-commit.policy.kind' = 'success-file'
            )
        """)
        
        logger.info("Parquet sink table created")
    
    def execute_pipeline(self):
        """Execute the streaming pipeline"""
        logger.info("Starting CDC to Parquet streaming...")
        logger.info("Capturing both BEFORE and AFTER data")
        
        result = self.table_env.execute_sql("""
            INSERT INTO sales_cdc_staging
            SELECT 
                -- After data
                after_id,
                after_product_name,
                after_category,
                after_price,
                after_quantity,
                after_sale_date,
                after_created_at,
                
                -- Before data
                before_id,
                before_product_name,
                before_category,
                before_price,
                before_quantity,
                before_sale_date,
                before_created_at,
                
                -- Metadata
                event_time as event_timestamp,
                op,
                source_db,
                source_table,
                txId,
                lsn,
                
                DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt
            FROM source_sales_cdc
            WHERE op IN ('c', 'r', 'u', 'd')
        """)
        
        job_id = result.get_job_client().get_job_id()
        logger.info(f"Job submitted: {job_id}")
        logger.info(f"Writing to: s3a://{self.bucket}/staging/sales_cdc/")
        logger.info(f"Flink Web UI: http://localhost:8082")
        logger.info("Pipeline running (Press Ctrl+C to stop)...")
        
        try:
            result.wait()
        except KeyboardInterrupt:
            logger.info("⏹️  Pipeline stopped by user")
            sys.exit(0)
    
    def run(self):
        """Main execution method"""
        try:
            self.create_environment()
            self.create_cdc_source_table()
            self.create_parquet_sink_table()
            self.execute_pipeline()
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == '__main__':
    pipeline = CDCToParquetPipeline()
    pipeline.run()
