#!/usr/bin/env python3
"""
CDC to Pinot Streaming Pipeline
Captures both 'before' and 'after' data for full CDC history
Writes to Pinot via Kafka for real-time analytics
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


class CDCToPinotPipeline:
    def __init__(self):
        self.table_env = None
        self.flink_home = "/home/your_username/pipeline/flink-1.18.1"
        
        # Configuration
        self.kafka_servers = 'localhost:9092'
        self.source_kafka_topic = 'mydb.public.source_sales'
        
        # Pinot ingestion topics (Pinot will consume from these)
        self.pinot_topic_current = 'pinot_sales_current'  # Current state 
        self.pinot_topic_history = 'pinot_sales_history'  # Full CDC history
    
    def create_environment(self):
        """Create Flink Table Environment"""
        logger.info("Creating Flink environment...")
        
        os.environ['FLINK_HOME'] = self.flink_home
        
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.table_env = TableEnvironment.create(env_settings)
        
        # Required JARs for Kafka
        jars = [
            f"{self.flink_home}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
            f"{self.flink_home}/lib/flink-json-1.18.1.jar"
        ]
        
        # Validate JARs
        missing = [j for j in jars if not os.path.exists(j)]
        if missing:
            raise FileNotFoundError(f"Missing JARs: {missing}")
        
        # Add JARs
        jar_paths = ";".join([f"file://{jar}" for jar in jars])
        self.table_env.get_config().set("pipeline.jars", jar_paths)
        
        # Configuration
        config = self.table_env.get_config()
        config.set("pipeline.name", "CDC-to-Pinot-Streaming-History")
        config.set("execution.checkpointing.interval", "60s")
        config.set("parallelism.default", "1")
        
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
                'topic' = '""" + self.source_kafka_topic + """',
                'properties.bootstrap.servers' = '""" + self.kafka_servers + """',
                'properties.group.id' = 'flink-cdc-to-pinot',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'raw'
            )
        """)
        
        # Create view to parse BOTH before and after
        self.table_env.execute_sql("""
            CREATE TEMPORARY VIEW source_sales_cdc AS
            SELECT 
                -- AFTER data (for INSERT/UPDATE and current state)
                CAST(JSON_VALUE(raw_message, '$.payload.after.id') AS INT) as after_id,
                JSON_VALUE(raw_message, '$.payload.after.product_name') as after_product_name,
                JSON_VALUE(raw_message, '$.payload.after.category') as after_category,
                CAST(JSON_VALUE(raw_message, '$.payload.after.price') AS DOUBLE) as after_price,
                CAST(JSON_VALUE(raw_message, '$.payload.after.quantity') AS INT) as after_quantity,
                CAST(JSON_VALUE(raw_message, '$.payload.after.sale_date') AS BIGINT) as after_sale_date,
                CAST(JSON_VALUE(raw_message, '$.payload.after.created_at') AS BIGINT) as after_created_at,
                
                -- BEFORE data (for DELETE and history)
                CAST(JSON_VALUE(raw_message, '$.payload.before.id') AS INT) as before_id,
                JSON_VALUE(raw_message, '$.payload.before.product_name') as before_product_name,
                JSON_VALUE(raw_message, '$.payload.before.category') as before_category,
                CAST(JSON_VALUE(raw_message, '$.payload.before.price') AS DOUBLE) as before_price,
                CAST(JSON_VALUE(raw_message, '$.payload.before.quantity') AS INT) as before_quantity,
                CAST(JSON_VALUE(raw_message, '$.payload.before.sale_date') AS BIGINT) as before_sale_date,
                CAST(JSON_VALUE(raw_message, '$.payload.before.created_at') AS BIGINT) as before_created_at,
                
                -- Operation type and metadata
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
    
    def create_pinot_current_state_sink(self):
        """
        Create Kafka sink for current state table in Pinot
        This maintains the current state of records (like a dimension table)
        """
        logger.info("Creating Pinot current state sink...")
        
        self.table_env.execute_sql(f"""
            CREATE TABLE pinot_sales_current_sink (
                id INT,
                product_name STRING,
                category STRING,
                price DOUBLE,
                quantity INT,
                sale_date BIGINT,
                created_at BIGINT,
                last_updated_ts BIGINT,
                is_deleted BOOLEAN,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = '{self.pinot_topic_current}',
                'properties.bootstrap.servers' = '{self.kafka_servers}',
                'key.format' = 'json',
                'value.format' = 'json'
            )
        """)
        
        logger.info("Pinot current state sink created")
    
    def create_pinot_history_sink(self):
        """
        Create Kafka sink for CDC history table in Pinot
        This maintains full CDC history with before/after data
        """
        logger.info("Creating Pinot CDC history sink...")
        
        self.table_env.execute_sql(f"""
            CREATE TABLE pinot_sales_history_sink (
                record_id INT,
                
                -- After data (for INSERT/UPDATE)
                after_id INT,
                after_product_name STRING,
                after_category STRING,
                after_price DOUBLE,
                after_quantity INT,
                after_sale_date BIGINT,
                after_created_at BIGINT,
                
                -- Before data (for UPDATE/DELETE)
                before_id INT,
                before_product_name STRING,
                before_category STRING,
                before_price DOUBLE,
                before_quantity INT,
                before_sale_date BIGINT,
                before_created_at BIGINT,
                
                -- CDC metadata
                op STRING,
                op_description STRING,
                event_timestamp BIGINT,
                source_db STRING,
                source_table STRING,
                tx_id BIGINT,
                lsn BIGINT,
                processing_time BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.pinot_topic_history}',
                'properties.bootstrap.servers' = '{self.kafka_servers}',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
        """)
        
        logger.info("Pinot CDC history sink created")
    
    def execute_current_state_pipeline(self):
        """
        Execute pipeline for current state table
        Maintains current view of data (upserts and deletes)
        """
        logger.info("Starting current state pipeline...")
        
        result = self.table_env.execute_sql(f"""
            INSERT INTO pinot_sales_current_sink
            SELECT 
                COALESCE(after_id, before_id) as id,
                after_product_name as product_name,
                after_category as category,
                after_price as price,
                after_quantity as quantity,
                after_sale_date as sale_date,
                after_created_at as created_at,
                ts_ms as last_updated_ts,
                CASE WHEN op = 'd' THEN TRUE ELSE FALSE END as is_deleted
            FROM source_sales_cdc
            WHERE op IN ('c', 'r', 'u', 'd')
        """)
        
        logger.info(f"Current state pipeline started")
        logger.info(f"Writing to Kafka topic: {self.pinot_topic_current}")
        return result
    
    def execute_history_pipeline(self):
        """
        Execute pipeline for CDC history table
        Maintains full audit trail of all changes
        """
        logger.info("Starting CDC history pipeline...")
        
        result = self.table_env.execute_sql(f"""
            INSERT INTO pinot_sales_history_sink
            SELECT 
                -- Use after_id for c/r/u, before_id for d
                COALESCE(after_id, before_id) as record_id,
                
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
                op,
                CASE 
                    WHEN op = 'c' THEN 'CREATE'
                    WHEN op = 'r' THEN 'READ'
                    WHEN op = 'u' THEN 'UPDATE'
                    WHEN op = 'd' THEN 'DELETE'
                    ELSE 'UNKNOWN'
                END as op_description,
                ts_ms as event_timestamp,
                source_db,
                source_table,
                txId as tx_id,
                lsn,
                UNIX_TIMESTAMP() * 1000 as processing_time
            FROM source_sales_cdc
            WHERE op IN ('c', 'r', 'u', 'd')
        """)
        
        logger.info(f"CDC history pipeline started")
        logger.info(f"Writing to Kafka topic: {self.pinot_topic_history}")
        return result
    
    def run(self):
        """Main execution method"""
        try:
            logger.info("=" * 70)
            logger.info("CDC to Pinot Streaming Pipeline")
            logger.info("=" * 70)
            
            self.create_environment()
            self.create_cdc_source_table()
            self.create_pinot_history_sink()
            logger.info("\nStarting pipeline:")
            logger.info(f"CDC History → {self.pinot_topic_history}")
            result = self.execute_history_pipeline()
            # result = self.execute_current_state_pipeline()
            # For history state, we need a separate statement execution
            # In prod env you might want to run these as separate jobs
            job_id = result.get_job_client().get_job_id()
            logger.info("=" * 70)
            logger.info(f"Job ID: {job_id}")
            logger.info(f"Source: {self.source_kafka_topic}")
            logger.info(f"Sink History: {self.pinot_topic_history}")
            logger.info(f"Flink Web UI: http://localhost:8082")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == '__main__':
    pipeline = CDCToPinotPipeline()
    pipeline.run()
