#!/bin/bash

set -e

PINOT_HOME="/opt/apache-pinot"
CONFIG_DIR="/home/shikhammad.ayyubov/pipeline/scripts/pinot_auto/generated_configs"
CONTROLLER_HOST="localhost"
CONTROLLER_PORT="9000"

echo "=========================================="
echo "Pinot Tables Setup (Auto-generated)"
echo "=========================================="

# Check if Pinot is running
echo "Checking Pinot services..."
if ! curl -s http://${CONTROLLER_HOST}:${CONTROLLER_PORT}/health > /dev/null; then
    echo " Pinot Controller is not running"
    exit 1
fi
echo " Pinot Controller is running"

# Create Kafka topics
echo ""
echo "Creating Kafka topics..."
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pinot_sales_current --partitions 3 --replication-factor 1 --config cleanup.policy=compact 2>/dev/null && echo " Topic created" || echo "  Topic already exists"
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic pinot_sales_history --partitions 3 --replication-factor 1 2>/dev/null && echo " Topic created" || echo "  Topic already exists"


echo ""
echo "=========================================="
echo "Setting up table: default.sales_current_3"
echo "=========================================="

echo "Adding table..."
${PINOT_HOME}/bin/pinot-admin.sh AddTable \
    -tableConfigFile ${CONFIG_DIR}/sales_current_3_table.json \
    -schemaFile ${CONFIG_DIR}/sales_current_3_schema.json \
    -controllerHost ${CONTROLLER_HOST} \
    -controllerPort ${CONTROLLER_PORT} \
    -database default \
    -exec

echo "✓ Table added: default.sales_current_3_REALTIME"


echo ""
echo "=========================================="
echo "Setting up table: default.sales_history_3"
echo "=========================================="

echo "Adding table..."
${PINOT_HOME}/bin/pinot-admin.sh AddTable \
    -tableConfigFile ${CONFIG_DIR}/sales_history_3_table.json \
    -schemaFile ${CONFIG_DIR}/sales_history_3_schema.json \
    -controllerHost ${CONTROLLER_HOST} \
    -controllerPort ${CONTROLLER_PORT} \
    -database default \
    -exec

echo "✓ Table added: default.sales_history_3_REALTIME"

echo ""
echo "=========================================="
echo " All tables configured successfully!"
echo "=========================================="
