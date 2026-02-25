#!/usr/bin/env python3
"""
Pinot Configuration Generator
Generates {table_name}_schema.json and {table_name}_table.json files from simple YAML configuration
"""

import yaml
import json
import os
from pathlib import Path


class PinotConfigGenerator:
    """Generate Pinot schema and table configurations from YAML"""
    
    def __init__(self, yaml_file="pinot_tables.yaml"):
        self.yaml_file = yaml_file
        self.config = None
        self.output_dir = None
    
    def load_config(self):
        """Load YAML configuration"""
        with open(self.yaml_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Set output directory
        self.output_dir = self.config.get('global', {}).get('output_dir', './generated_configs')
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        print(f" Loaded configuration from {self.yaml_file}")
        print(f" Output directory: {self.output_dir}")
    
    def generate_schema(self, table_name, table_config):
        """Generate Pinot schema JSON"""
        schema = {
            "schemaName": table_name,
            "dimensionFieldSpecs": [],
            "metricFieldSpecs": [],
            "dateTimeFieldSpecs": []
        }
        
        # Add primary key if exists (for upsert tables)
        if table_config.get('mode') == 'upsert' and 'upsert' in table_config:
            schema["primaryKeyColumns"] = table_config['upsert']['primary_key']
        
        # Process columns
        for col in table_config['columns']:
            field_spec = {
                "name": col['name'],
                "dataType": col['type']
            }
            
            # Add default value if specified
            if 'default' in col:
                field_spec['defaultNullValue'] = col['default']
            
            # Add to appropriate section
            if col['field_type'] == 'dimension':
                schema['dimensionFieldSpecs'].append(field_spec)
            
            elif col['field_type'] == 'metric':
                schema['metricFieldSpecs'].append(field_spec)
            
            elif col['field_type'] == 'datetime':
                datetime_spec = {
                    "name": col['name'],
                    "dataType": col['type'],
                    "format": col.get('format', '1:MILLISECONDS:EPOCH'),
                    "granularity": col.get('granularity', '1:MILLISECONDS')
                }
                schema['dateTimeFieldSpecs'].append(datetime_spec)
        
        return schema
    
    def generate_table_config(self, table_name, table_config):
        """Generate Pinot table configuration JSON"""
        
        time_col = table_config['time_column']
        kafka = table_config['kafka']
        retention = table_config['retention']
        advanced = table_config.get('advanced', {})
        
         

        # Base table configuration
        table = {
            "tableName": table_name,
            "tableType": table_config['table_type'],
            "segmentsConfig": {
                "timeColumnName": time_col['name'],
                "timeType": time_col['type'],
                "retentionTimeUnit": retention['time_unit'],
                "retentionTimeValue": str(retention['time_value']),
                "segmentPushType": "APPEND",
                "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
                "schemaName": table_name,
                "replication": str(advanced.get('replication', 1)),
                "replicasPerPartition": str(advanced.get('replication', 1))
            },
            "tenants": {},
            "tableIndexConfig": {
                "loadMode": advanced.get('load_mode', 'MMAP'),
                "nullHandlingEnabled": True
            },
            "metadata": {}
        }
        
        # Add upsert config if mode is upsert
        if table_config.get('mode') == 'upsert' and 'upsert' in table_config:
            upsert = table_config['upsert']
            table['upsertConfig'] = {
                "mode": "FULL",
                "comparisonColumn": upsert['comparison_column']
            }
            if upsert.get('enable_snapshot'):
                table['upsertConfig']['enableSnapshot'] = True
        
        # Ingestion config
        stream_config = {
            "streamType": "kafka",
            "stream.kafka.consumer.type": kafka.get('consumer_type', 'lowlevel'),
            "stream.kafka.topic.name": kafka['topic'],
            "stream.kafka.broker.list": kafka['bootstrap_servers'],
            "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
            "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
            "realtime.segment.flush.threshold.rows": str(advanced.get('flush_threshold_rows', 100000)),
            "stream.kafka.consumer.prop.auto.offset.reset": kafka.get('offset_reset', 'smallest')
        }
        
        table['ingestionConfig'] = {
            "streamIngestionConfig": {
                "streamConfigMaps": [stream_config]
            }
        }

        if table_config.get('mode') == 'upsert':
            table["routing"] = {
                "instanceSelectorType": "strictReplicaGroup"
            }
        
        return table
    
    def generate_kafka_topic_commands(self, table_name, table_config):
        """Generate Kafka topic creation command"""
        kafka = table_config['kafka']
        
        cmd = f"kafka-topics --bootstrap-server {kafka['bootstrap_servers']} --create"
        cmd += f" --if-not-exists --topic {kafka['topic']}"
        cmd += f" --partitions {kafka.get('partitions', 3)}"
        cmd += f" --replication-factor {kafka.get('replication_factor', 1)}"
        
        if kafka.get('cleanup_policy'):
            cmd += f" --config cleanup.policy={kafka['cleanup_policy']}"
        
        return cmd
    
    def generate_all(self):
        """Generate all configurations"""
        if not self.config:
            self.load_config()
        
        tables = self.config['tables']
        kafka_commands = []
        
        print("Generating Pinot Configurations")
        
        for table_name, table_config in tables.items():
            print(f"Processing table: {table_name}")
            print(f"  Description: {table_config.get('description', 'N/A')}")
            print(f"  Mode: {table_config.get('mode', 'append')}")
            
            # Generate schema
            schema = self.generate_schema(table_name, table_config)
            schema_file = os.path.join(self.output_dir, f"{table_name}_schema.json")
            with open(schema_file, 'w') as f:
                json.dump(schema, f, indent=2)
            print(f"Generated: {schema_file}")
            
            # Generate table config
            table = self.generate_table_config(table_name, table_config)
            table_file = os.path.join(self.output_dir, f"{table_name}_table.json")
            with open(table_file, 'w') as f:
                json.dump(table, f, indent=2)
            print(f"Generated: {table_file}")
            
            # Generate Kafka topic command
            kafka_cmd = self.generate_kafka_topic_commands(table_name, table_config)
            kafka_commands.append(kafka_cmd)
            
            print()
        
        # Generate setup script
        self.generate_setup_script(kafka_commands)
        
        print(f"{'='*70}")
        print(f" All configurations generated in: {self.output_dir}")
        print(f"{'='*70}\n")
        print("Next steps:")
        print(f"1. Review generated files in {self.output_dir}/")
        print(f"2. Run: bash {self.output_dir}/setup_pinot_tables.sh")
        print()
    
    def generate_setup_script(self, kafka_commands):
        """Generate bash setup script"""
        
        global_config = self.config.get('global', {})
        controller_host, controller_port = global_config.get('pinot_controller', 'localhost:9000').split(':')
        
        script = f"""#!/bin/bash

set -e

PINOT_HOME="/opt/apache-pinot"
CONFIG_DIR="{os.path.abspath(self.output_dir)}"
CONTROLLER_HOST="{controller_host}"
CONTROLLER_PORT="{controller_port}"

echo "Pinot Tables Setup (Auto-generated)"

# Check if Pinot is running
echo "Checking Pinot services..."
if ! curl -s http://${{CONTROLLER_HOST}}:${{CONTROLLER_PORT}}/health > /dev/null; then
    echo " Pinot Controller is not running"
    exit 1
fi
echo " Pinot Controller is running"

# Create Kafka topics
echo ""
echo "Creating Kafka topics..."
"""
        
        for cmd in kafka_commands:
            script += f'{cmd} 2>/dev/null && echo " Topic created" || echo "  Topic already exists"\n'
        
        for table_name, table_config in self.config['tables'].items():
            database_name = table_config['database']
            script += f"""

echo ""
echo "Setting up table: {database_name}.{table_name}"

echo "Adding table..."
${{PINOT_HOME}}/bin/pinot-admin.sh AddTable \\
    -tableConfigFile ${{CONFIG_DIR}}/{table_name}_table.json \\
    -schemaFile ${{CONFIG_DIR}}/{table_name}_schema.json \\
    -controllerHost ${{CONTROLLER_HOST}} \\
    -controllerPort ${{CONTROLLER_PORT}} \\
    -database {database_name} \\
    -exec

echo "Table added: {database_name}.{table_name}_REALTIME"

"""
        
        script += """
echo "All tables configured successfully"
"""
        
        script_file = os.path.join(self.output_dir, "setup_pinot_tables.sh")
        with open(script_file, 'w') as f:
            f.write(script)
        
        # make file executable
        os.chmod(script_file, 0o755)
        
        print(f"   Generated: {script_file}")


def main():
    """Main entry point"""
    print("""Pinot Configuration Generator from YAML                   
             Simplify Pinot table management with YAML configs                  
    """)
    
    generator = PinotConfigGenerator("pinot_tables.yaml")
    generator.generate_all()
    
    print("Done!")


if __name__ == "__main__":
    main()
