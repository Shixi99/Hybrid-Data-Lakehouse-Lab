#!/bin/bash

KAFKA_CONNECT_URL="http://localhost:8084"
CONNECTOR_NAME=$1

if [ -z "$CONNECTOR_NAME" ]; then
    echo "Usage: ./delete-connector.sh <connector-name>"
    exit 1
fi

echo "Deleting connector: $CONNECTOR_NAME"
curl -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME"
echo ""
echo "Deleted: $CONNECTOR_NAME"
