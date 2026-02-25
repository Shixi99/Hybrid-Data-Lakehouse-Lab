#!/usr/bin/env bash

### ===== CHECK SERVICE STATUS =====

echo "=========================================="
echo "Data Lake Infrastructure Status"
echo "=========================================="
echo ""

check_service() {
  local name=$1
  local check_cmd=$2
  
  printf "%-20s" "$name:"
  if eval "$check_cmd" >/dev/null 2>&1; then
    echo "✓ Running"
    return 0
  else
    echo "✗ Stopped"
    return 1
  fi
}

check_http() {
  local name=$1
  local url=$2
  
  printf "%-20s" "$name:"
  if curl -s "$url" >/dev/null 2>&1; then
    echo "✓ Running ($url)"
    return 0
  else
    echo "✗ Stopped"
    return 1
  fi
}

# Core services
check_service "Zookeeper" "pgrep -f zookeeper"
check_service "Kafka" "pgrep -f kafka.Kafka"

# Pinot
check_http "Pinot Controller" "http://localhost:9000/health"
check_http "Pinot Broker" "http://localhost:8099/health"
check_service "Pinot Server" "pgrep -f 'pinot.*StartServer'"

# Processing
check_http "Flink" "http://localhost:8082"
check_service "Kafka Connect" "pgrep -f ConnectDistributed"

# UI
check_http "Kafka UI" "http://localhost:8083"

echo ""
echo "=========================================="
echo "Service URLs:"
echo "=========================================="
echo "Kafka UI:       http://localhost:8083"
echo "Flink UI:       http://localhost:8082"
echo "Kafka Connect:  http://localhost:8084"
echo "Pinot UI:       http://localhost:9000"
echo "=========================================="
