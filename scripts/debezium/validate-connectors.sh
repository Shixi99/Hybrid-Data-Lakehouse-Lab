#!/bin/bash

CONFIG_FILE="${1:-../../connectors/connectors.yaml}"

echo "Validating $CONFIG_FILE"
echo "=========================================="

# Check if file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "✗ Error: File not found"
    exit 1
fi

# Check YAML syntax
if ! yq eval "$CONFIG_FILE" > /dev/null 2>&1; then
    echo "✗ Invalid YAML syntax"
    exit 1
fi

echo "✓ YAML syntax valid"

# Check structure
CONNECTOR_COUNT=$(yq eval '.connectors | length' "$CONFIG_FILE")
echo "✓ Found $CONNECTOR_COUNT connector(s)"

# List connectors
echo ""
echo "Connectors defined:"
yq eval '.connectors[].name' "$CONFIG_FILE" | while read name; do
    echo "  - $name"
done

echo ""
echo "✓ Configuration valid"
