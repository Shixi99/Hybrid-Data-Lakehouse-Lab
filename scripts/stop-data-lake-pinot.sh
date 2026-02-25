#!/usr/bin/env bash
set -euo pipefail

### ===== CONFIG =====
BASE_DIR="$HOME/pipeline"
KAFKA_DIR="$BASE_DIR/kafka_2.12-2.8.2"
FLINK_DIR="$BASE_DIR/flink-1.18.1"

### ===== HELPERS =====
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

### ===== STOP SERVICES =====

log "Stopping all services..."

# 1. Stop Flink jobs first
log "Stopping Flink jobs..."
if pgrep -f "cdc_to_pinot" >/dev/null 2>&1; then
  pkill -f "cdc_to_pinot" || true
fi

# 2. Stop Flink cluster
log "Stopping Flink cluster..."
if pgrep -f "flink" >/dev/null 2>&1; then
  "$FLINK_DIR/bin/stop-cluster.sh" || true
  sleep 2
fi

# 3. Stop Kafka Connect (Debezium)
log "Stopping Kafka Connect..."
if pgrep -f "ConnectDistributed" >/dev/null 2>&1; then
  pkill -f "ConnectDistributed" || true
  sleep 2
fi

# 4. Stop Apache Pinot components (in reverse order)
log "Stopping Apache Pinot..."

# Stop Server
if  pgrep -f "pinot.*StartServer" >/dev/null 2>&1; then
  log "Stopping Pinot Server..."
  pkill -f "pinot.*StartServer" || true
  sleep 2
fi

# Stop Broker
if pgrep -f "pinot.*StartBroker" >/dev/null 2>&1; then
  log "Stopping Pinot Broker..."
   pkill -f "pinot.*StartBroker" || true
  sleep 2
fi

# Stop Controller
if pgrep -f "pinot.*StartController" >/dev/null 2>&1; then
  log "Stopping Pinot Controller..."
   pkill -f "pinot.*StartController" || true
  sleep 2
fi

# 5. Stop Kafka UI
log "Stopping Kafka UI..."
if pgrep -f "kafka-ui" >/dev/null 2>&1; then
  pkill -f "kafka-ui" || true
  sleep 2
fi

# 6. Stop Kafka
log "Stopping Kafka..."
if pgrep -f "kafka.Kafka" >/dev/null 2>&1; then
  "$KAFKA_DIR/bin/kafka-server-stop.sh" || true
  sleep 5
fi

# 7. Stop Zookeeper (last)
log "Stopping Zookeeper..."
if pgrep -f "zookeeper" >/dev/null 2>&1; then
  "$KAFKA_DIR/bin/zookeeper-server-stop.sh" || true
  sleep 2
fi

sleep 10

### ===== VERIFY =====
log "Verifying all services stopped..."

STILL_RUNNING=0

if pgrep -f "zookeeper" >/dev/null 2>&1; then
  log "Zookeeper still running"
  STILL_RUNNING=1
fi

if pgrep -f "kafka.Kafka" >/dev/null 2>&1; then
  log "Kafka still running"
  STILL_RUNNING=1
fi

if pgrep -f "pinot" | grep -v "$(basename "$0")" >/dev/null 2>&1; then
  log "Pinot still running"
  STILL_RUNNING=1
fi

if pgrep -f "flink" >/dev/null 2>&1; then
  log "Flink still running"
  STILL_RUNNING=1
fi

if [ $STILL_RUNNING -eq 0 ]; then
  log "All services stopped successfully"
else
  log "Some services still running (use 'ps aux | grep <service>' to check)"
fi
