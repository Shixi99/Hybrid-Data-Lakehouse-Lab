#!/bin/bash

KAFKA_CONNECT_URL="http://localhost:8084"
CONFIG_FILE="${1:-../../connectors/connectors.yaml}"

echo "Connector Status Report"
echo "=========================================="
echo ""

# Get all connector names from YAML
CONNECTOR_NAMES=$(yq eval -o=json "$CONFIG_FILE" | jq -r '.connectors[].name')

for name in $CONNECTOR_NAMES; do
    echo "$name"
    
    STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$name/status" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ ! -z "$STATUS" ]; then
        CONNECTOR_STATE=$(echo "$STATUS" | jq -r '.connector.state')
        TASK_STATE=$(echo "$STATUS" | jq -r '.tasks[0].state // "N/A"')
        
        if [ "$CONNECTOR_STATE" == "RUNNING" ]; then
            echo "   Connector: ✓ RUNNING"
        else
            echo "   Connector: ✗ $CONNECTOR_STATE"
        fi
        
        if [ "$TASK_STATE" == "RUNNING" ]; then
            echo "   Task:      ✓ RUNNING"
        else
            echo "   Task:      ✗ $TASK_STATE"
        fi
    else
        echo "   ✗ NOT DEPLOYED"
    fi
    
    echo ""
done

echo "=========================================="
