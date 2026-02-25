#!/usr/bin/env bash
set -euo pipefail

### ===== CONFIG =====
BASE_DIR="$HOME/pipeline"
KAFKA_DIR="$BASE_DIR/kafka_2.12-2.8.2"
FLINK_DIR="$BASE_DIR/flink-1.18.1"
KAFKA_UI_DIR="$BASE_DIR/kafka-ui"
SCRIPTS_DIR="$BASE_DIR/scripts"
DEBEZIUM_SCRIPTS="$SCRIPTS_DIR/debezium"
FLINK_PIPELINES=("cdc_to_parquet" "cdc_to_pinot_current" "cdc_to_pinot_history")
LOG_DIR="$BASE_DIR/logs"
PINOT_HOME="/opt/apache-pinot"
PINOT_DATA="/var/pinot"

mkdir -p "$LOG_DIR"

### ===== HELPERS =====
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

is_running() {
  pgrep -f "$1" >/dev/null 2>&1
}

sleep_wait() {
  log "Waiting $1 seconds..."
  sleep "$1"
}

### ===== 1. ZOOKEEPER =====
if is_running "zookeeper"; then
  log "Zookeeper already running"
else
  log "Starting Zookeeper..."
  "$KAFKA_DIR/bin/zookeeper-server-start.sh" -daemon \
    "$KAFKA_DIR/config/zookeeper.properties"
  sleep_wait 5
fi

### ===== 2. KAFKA =====
if is_running "kafka.Kafka"; then
  log "Kafka already running"
else
  log "Starting Kafka..."
  "$KAFKA_DIR/bin/kafka-server-start.sh" -daemon \
    "$KAFKA_DIR/config/server.properties"
  sleep_wait 10
fi

### ===== 3. KAFKA UI =====
if is_running "kafka-ui"; then
  log "Kafka UI already running"
else
  log "Starting Kafka UI..."
  (
    cd "$KAFKA_UI_DIR"
    nohup java -jar kafka-ui.jar \
      > "$LOG_DIR/kafka-ui.log" 2>&1 &
  )
  sleep_wait 5
fi

### ===== 4. APACHE PINOT =====
log "Starting Apache Pinot components..."

# 4a. Pinot Controller
if pgrep -f "pinot.*StartController" >/dev/null 2>&1; then
  log "Pinot Controller already running"
else
  log "Starting Pinot Controller..."
   nohup "$PINOT_HOME/bin/pinot-admin.sh" StartController \
    -zkAddress localhost:2181 \
    -clusterName PinotCluster \
    -controllerPort 9000 \
    -dataDir "$PINOT_DATA/controller" \
    > "$LOG_DIR/pinot-controller.log" 2>&1 &
  sleep_wait 10
fi

# 4b. Pinot Broker
if pgrep -f "pinot.*StartBroker" >/dev/null 2>&1; then
  log "Pinot Broker already running"
else
  log "Starting Pinot Broker..."
   nohup "$PINOT_HOME/bin/pinot-admin.sh" StartBroker \
    -zkAddress localhost:2181 \
    -clusterName PinotCluster \
    > "$LOG_DIR/pinot-broker.log" 2>&1 &
  sleep_wait 10
fi

# 4c. Pinot Server
if pgrep -f "pinot.*StartServer" >/dev/null 2>&1; then
  log "Pinot Server already running"
else
  log "Starting Pinot Server..."
   nohup "$PINOT_HOME/bin/pinot-admin.sh" StartServer \
    -zkAddress localhost:2181 \
    -clusterName PinotCluster \
    -dataDir "$PINOT_DATA/server" \
    -segmentDir "$PINOT_DATA/server/segments" \
    > "$LOG_DIR/pinot-server.log" 2>&1 &
  sleep_wait 10
fi

# Verify Pinot is healthy
log "Verifying Pinot health..."
if curl -s http://localhost:9000/health >/dev/null && \
   curl -s http://localhost:8099/health >/dev/null; then
  log "✓ Pinot is healthy"
else
  log "⚠ Pinot health check failed"
fi

### ===== 5. FLINK CLUSTER =====
if is_running "flink"; then
  log "Flink already running"
else
  log "Starting Flink cluster..."
  "$FLINK_DIR/bin/start-cluster.sh"
  sleep_wait 10
fi

### ===== 6. DEBEZIUM KAFKA CONNECT =====
if is_running "ConnectDistributed"; then
  log "Kafka Connect already running"
else
  log "Starting Debezium Kafka Connect..."
  (
    cd "$KAFKA_DIR"
    nohup bin/connect-distributed.sh \
      config/connect-distributed.properties \
      > "$LOG_DIR/kafka-connect.log" 2>&1 &
  )
  sleep_wait 15
fi

### ===== 7. DEPLOY CONNECTORS =====
log "Deploying Debezium connectors..."
(
  cd "$DEBEZIUM_SCRIPTS"
  ./deploy-connectors.sh
)

### ===== 8. RUN FLINK PIPELINES =====
#log "Starting Flink pipeline: $FLINK_PIPELINE"
#(
#  cd "$SCRIPTS_DIR"
#  ./run_flink_pipeline.sh "$FLINK_PIPELINE"
#)


for PIPELINE in "${FLINK_PIPELINES[@]}"; do
  log "Starting Flink pipeline: $PIPELINE"
  (
    cd "$SCRIPTS_DIR"
    ./run_flink_pipeline.sh "$PIPELINE"
  )
done



### ===== SUMMARY =====
log "✅ Stack started successfully"
echo ""
echo "=========================================="
echo "Service URLs:"
echo "=========================================="
echo "Zookeeper:      localhost:2181"
echo "Kafka:          localhost:9092"
echo "Kafka UI:       http://localhost:8080"
echo "Flink UI:       http://localhost:8081"
echo "Kafka Connect:  http://localhost:8083"
echo "Pinot UI:       http://localhost:9000"
echo "Pinot Broker:   localhost:8099"
echo "=========================================="
