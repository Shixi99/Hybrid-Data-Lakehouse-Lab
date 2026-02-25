#!/bin/bash

KAFKA_CONNECT_URL="http://localhost:8084"
CONFIG_FILE="${1:-../../connectors/connectors.yaml}"
CONNECTOR_NAME=$2

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file $CONFIG_FILE not found"
    exit 1
fi

deploy_connector() {
    local connector_json=$1
    local name=$(echo "$connector_json" | jq -r '.name')
    
    echo "Deploying: $name"
    
    # Check if connector exists
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$KAFKA_CONNECT_URL/connectors/$name")
    
    if [ "$HTTP_CODE" == "200" ]; then
        echo "  ↻ Updating existing connector..."
        RESPONSE=$(curl -s -X PUT "$KAFKA_CONNECT_URL/connectors/$name/config" \
            -H "Content-Type: application/json" \
            -d "$(echo "$connector_json" | jq -c '.config')")
    else
        echo "  ✓ Creating new connector..."
        RESPONSE=$(curl -s -X POST "$KAFKA_CONNECT_URL/connectors" \
            -H "Content-Type: application/json" \
            -d "$connector_json")
    fi
    
    # Check for errors
    if echo "$RESPONSE" | jq -e '.error_code' > /dev/null 2>&1; then
        echo "  ✗ Error: $(echo "$RESPONSE" | jq -r '.message')"
        return 1
    fi
    
    # Check status
    sleep 1
    STATUS=$(curl -s "$KAFKA_CONNECT_URL/connectors/$name/status" | jq -r '.connector.state')
    echo "  Status: $STATUS"
    echo ""
}

# Convert YAML to JSON
CONNECTORS_JSON=$(yq eval -o=json "$CONFIG_FILE")

if [ -z "$CONNECTOR_NAME" ]; then
    # Deploy all connectors
    echo "Deploying all connectors from $CONFIG_FILE"
    echo "=========================================="
    echo ""
    
    CONNECTOR_COUNT=$(echo "$CONNECTORS_JSON" | jq '.connectors | length')
    echo "Found $CONNECTOR_COUNT connector(s)"
    echo ""
    
    echo "$CONNECTORS_JSON" | jq -c '.connectors[]' | while read connector; do
        deploy_connector "$connector"
    done
else
    # Deploy specific connector
    echo "Deploying connector: $CONNECTOR_NAME"
    echo "=========================================="
    echo ""
    
    CONNECTOR=$(echo "$CONNECTORS_JSON" | jq -c ".connectors[] | select(.name == \"$CONNECTOR_NAME\")")
    
    if [ -z "$CONNECTOR" ]; then
        echo "Error: Connector '$CONNECTOR_NAME' not found in $CONFIG_FILE"
        exit 1
    fi
    
    deploy_connector "$CONNECTOR"
fi

echo "=========================================="
echo "Deployment complete!"
echo ""
echo "Active connectors:"
curl -s "$KAFKA_CONNECT_URL/connectors" | jq
