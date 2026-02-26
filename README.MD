# Hybrid Real-Time Lakehouse

A production-oriented data platform built entirely on open-source components, running on a single Ubuntu 22.04 server. The architecture combines real-time stream processing with a versioned batch lakehouse, both fed from the same Change Data Capture source.

This repository contains every script, configuration file, and connector definition used in the working implementation. Nothing is pseudocode or illustrative — these are the exact files from the running system.

---

## Architecture

```
                    PostgreSQL 16
                         |
                  Debezium 2.5 (CDC)
                         |
                    Kafka 2.8.2
                   (event backbone)
                         |
                   Flink 1.18.1
                  (stream routing)
                    /          \
                   /            \
       Kafka topics           MinIO (S3)
       (cleaned CDC)       Parquet files
            |              (Bronze layer)
            |                   |
         Pinot              Spark 3.5.1
         1.4.0           + Airflow 2.9
       (OLAP engine)      (SCD2 batch job)
            |                   |
         PowerBI         Iceberg tables
       (real-time)       + Nessie 0.105.7
                          (versioned catalog)
                                |
                           Trino 443
                         (federated SQL)
                                |
                             PowerBI
                           (analytical)
```

The real-time lane (left path) delivers millisecond-latency aggregations over continuously updating data. The batch lane (right path) builds a versioned, time-travel-capable lakehouse with full CDC history preserved as SCD Type 2 records.

Data flow in detail:

1. PostgreSQL logical replication streams every INSERT, UPDATE, and DELETE to Debezium.
2. Debezium publishes structured CDC events (with `before` and `after` payloads) to a Kafka topic.
3. Two Flink jobs consume from the same topic concurrently:
   - `cdc_to_pinot_current.py` routes cleaned events - only current state of data - to Kafka topic consumed by Pinot.
   - `cdc_to_pinot_history.py` routes cleaned events - history full chages - to Kafka topic consumed by Pinot.
   - `cdc_to_parquet.py` writes raw CDC events as partitioned Parquet files to MinIO.
4. Pinot ingests from Kafka in real time. One table maintains current state (upsert mode), another maintains the full audit trail (append mode).
5. Spark reads incremental Parquet files from MinIO, applies SCD Type 2 logic, and merges the results into Iceberg tables managed by Nessie.
6. Airflow orchestrates the Spark job with retry handling and incremental checkpointing.
7. Trino federates SQL queries across the Nessie/Iceberg lakehouse and the operational Postgres database.

---

## Medallion Layers

| Layer | Storage | Written by | Read by |
|---|---|---|---|
| Bronze | MinIO `s3a://streaming/staging/sales_cdc/` | Flink | Spark |
| Silver | MinIO `s3a://lakehouse/` (Iceberg) | Spark | Trino |
| Gold | Iceberg views / Trino queries | Trino | PowerBI, analysts |
| Real-time | Pinot segments (Kafka-backed) | Flink via Kafka | Pinot, PowerBI |

---

## Stack Versions

| Component | Version | Role |
|---|---|---|
| PostgreSQL | 16 | Source database |
| Python | 3.9 | All custom scripts |
| Debezium | 2.5.0.Final | CDC connector (Kafka Connect plugin) |
| Apache Kafka | 2.12-2.8.2 | Event backbone |
| Kafka UI | 0.7.2 | Topic inspection |
| Apache Flink | 1.18.1 | Stream processing |
| MinIO | latest | Object storage (S3-compatible) |
| Apache Spark | 3.5.1 | Batch SCD2 processing |
| Apache Airflow | 2.9 | Orchestration |
| Project Nessie | 0.105.7 | Iceberg catalog with versioning |
| Apache Iceberg | 1.10.0 | Open table format |
| Apache Trino | 443 | Federated SQL engine |
| Apache Pinot | 1.4.0 | Real-time OLAP |

---

## Repository Structure

```
pipeline/
|-- connectors/
|   `-- connectors.yaml              # Debezium connector definitions (all connectors in one file)
|
|-- scripts/
|   |-- start-data-lake-pinot.sh     # Master startup script for real-time components
|   |-- cdc_to_pinot_current.py              # Flink job: CDC current state of data -> Pinot via Kafka
|   |-- cdc_to_pinot_history.py              # Flink job: CDC full changes -> Pinot via Kafka
|   |-- cdc_to_parquet.py            # Flink job: CDC events -> Parquet files on MinIO
|   |
|   |-- debezium/
|   |   |-- deploy-connectors.sh     # Deploy all connectors from connectors.yaml
|   |   |-- check_status.sh          # Report connector and task health
|   |   |-- delete-connector.sh      # Remove a connector by name
|   |   `-- validate-connectors.sh   # Validate YAML syntax before deploying
|   |
|   `-- pinot_auto/
|       |-- pinot_tables.yaml        # Declarative table definitions
|       |-- generate_pinot_configs.py # Generates schema.json, table.json, setup.sh
|       |-- data_gen_pg2.py          # Continuous random data generator (Postgres) (for testing purpose)
|       `-- generated_configs/       # Output directory - auto-generated pinot table/schema configs and setup script for deployment
|
|-- libs/
|   `-- scd2_spark_processor.py      # Spark SCD2 processor (INSERT, UPDATE, DELETE)
|
|-- dags/
|   `-- cdc_scd2_dag.py              # Airflow DAG: check staging + run Spark
|
`-- kafka-ui/
    `-- application.yml              # Kafka UI configuration
```

---

## Prerequisites

- Ubuntu 22.04
- Java 17 (for Nessie and Kafka)
- Java 11 (for Pinot — must be set explicitly, Pinot 1.4.0 does not support Java 17)
- Python 3.9 (via Deadsnakes PPA)
- sudo access
- 16 GB RAM minimum, 100 GB disk

All components run on localhost. There is no Docker or Kubernetes dependency.

---

## Service Ports

| Service | Port | Notes |
|---|---|---|
| PostgreSQL | 5432 | |
| MinIO S3 API | 9002 | Used by Flink, Spark, Trino |
| MinIO Console | 9001 | Browser UI |
| Kafka Broker | 9092 | |
| Kafka Connect REST | 8084 | Debezium management API |
| Kafka UI | 8083 | Browser UI |
| Flink Web UI | 8082 | Job monitoring |
| Airflow Web UI | 8080 | DAG management |
| Trino Web UI / API | 8081 | Query interface |
| Nessie API | 19121 | REST catalog for Iceberg |
| Pinot Controller UI | 9000 | Browser UI |
| Pinot Broker | 8099 | Query endpoint |

---

## Installation Order

Components must be installed and started in dependency order. Services marked as systemd run persistently; the others are started by the master startup script.

```
1.  PostgreSQL 16          (systemd)
2.  Python 3.9             (system package)
3.  MinIO                  (systemd)
4.  Airflow 2.9            (systemd, depends on Python 3.9 venv)
5.  Spark 3.5.1            (manual install to /opt/spark)
6.  Nessie 0.105.7         (systemd, depends on PostgreSQL)
7.  Trino 443              (systemd, depends on Nessie)
8.  Kafka 2.8.2            (started by startup script)
9.  Debezium connector     (started by startup script via Kafka Connect)
10. Flink 1.18.1           (started by startup script)
11. Pinot 1.4.0            (manual start, depends on Kafka)
```


---

## Quick Start (after installation)

**Start real-time components:**

```bash
chmod +x ~/pipeline/scripts/start-data-lake-pinot.sh
~/pipeline/scripts/start-data-lake-pinot.sh
```

This script starts ZooKeeper, Kafka, Kafka UI, Flink, and Kafka Connect in order, deploys the Debezium connector, and submits the Flink CDC-to-Pinot pipeline.


**Manage Debezium connectors:**

```bash
cd ~/pipeline/scripts/debezium

./validate-connectors.sh                    # Validate connectors.yaml
./deploy-connectors.sh                      # Deploy all connectors
./check_status.sh                           # Check connector and task status
./delete-connector.sh postgres-connector    # Remove a specific connector
```

**Generate and deploy Pinot table configurations:**

```bash
cd ~/pipeline/scripts/pinot_auto
python3 generate_pinot_configs.py
bash generated_configs/setup_pinot_tables.sh
```

**Trigger the Spark SCD2 batch job:**

Navigate to the Airflow UI at `http://localhost:8080`, find the `scd2_sales_spark_processing` DAG, and trigger it manually. Or from the command line:

```bash
/opt/airflow_venv/bin/airflow dags trigger scd2_sales_spark_processing
```


## Connector Management

All Debezium connectors are defined in a single YAML file at `pipeline/connectors/connectors.yaml`. The deployment scripts read this file and interact with the Kafka Connect REST API.

To add a new connector, append a new entry to `connectors.yaml` and run `deploy-connectors.sh`. To update an existing connector, edit its config and run `deploy-connectors.sh` again — it detects existing connectors and sends a PUT instead of POST.

The scripts require `jq` and `yq` to be installed:

```bash
sudo apt install -y jq
sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/local/bin/yq
sudo chmod +x /usr/local/bin/yq
```

---

## Pinot Table Generator

Rather than writing Pinot's verbose JSON configuration by hand, table definitions are maintained in `pinot_tables.yaml` and converted to JSON by `generate_pinot_configs.py`. The generator outputs:

- `<table_name>_schema.json` — field specs for dimensions, metrics, and datetime columns
- `<table_name>_table.json` — ingestion config, segment config, upsert config (if applicable)
- `setup_pinot_tables.sh` — a runnable script that creates the Kafka topics and registers the tables

To add a new table, add an entry to `pinot_tables.yaml` and re-run the generator.

---

## SCD Type 2 Logic

The Spark processor (`libs/scd2_spark_processor.py`) handles all three CDC operation types:

- INSERT (`op=c` or `op=r`): appends a new row with `is_current=true`, `is_deleted=false`
- UPDATE (`op=u`): uses MD5 hash comparison to detect actual data changes, closes the existing current row by setting `is_current=false` and `effective_end_ts=event_time`, then appends the new version
- DELETE (`op=d`): uses the `before_` payload columns to identify the record, closes it with `is_current=false`, `is_deleted=true`

Incremental processing is managed via a checkpoint Iceberg table (`source_sales_scd2_checkpoint`) that stores the last processed timestamp and LSN. Each run reads only staging partitions created after the last checkpoint.

---

## Important Notes

**Path dependencies.** The startup script (`start-data-lake-pinot.sh`) and both Flink jobs use `$HOME/pipeline` as the base directory. If you clone this repository to a different path, update the `BASE_DIR` variable in the startup script and the `flink_home` attribute in the Flink Python scripts.

**Credentials.** All passwords in this repository are placeholders (`your_password_here`, `your_minio_secret_here`). Replace them before deploying. The Debezium connector password is in `connectors/connectors.yaml`. MinIO credentials appear in the Flink scripts, the Spark processor, the Airflow DAG, and the Trino catalog config.

**Java versions.** Nessie 0.105.7 and Trino 443 require Java 17. Pinot 1.4.0 requires Java 11. If Java 17 is your system default, set `JAVA_HOME` explicitly in `~/.bashrc` before the Pinot environment block.

**Nessie API version.** Trino 443 with Nessie 0.105.x must use `/api/v2` in the catalog URI. Using `/api/v1` causes catalog initialization to fail silently.

**Flink JAR names.** The Flink jobs reference specific JAR filenames in `~/pipeline/flink-1.18.1/lib/`. The JAR list in each script must match what is actually present in that directory. Download instructions are in the tutorial document.

**Spark extra JARs.** The Spark processor references JARs from `/opt/spark/jars-extra2/`. This directory and its contents must be created manually. The exact filenames are referenced directly in the script's `.config("spark.jars", ...)` call.

---

## Querying the Platform

**Pinot — real-time current state:**

```sql
SELECT category, SUM(price * quantity) AS revenue
FROM default.sales_current_3
WHERE is_deleted = false
GROUP BY category
ORDER BY revenue DESC
```

**Pinot — CDC audit trail:**

```sql
SELECT op_description, after_price, before_price, event_timestamp
FROM default.sales_history_3
WHERE record_id = 5042
ORDER BY event_timestamp ASC
```

**Trino — lakehouse historical query:**

```sql
SELECT id, product_name, price, effective_start_ts, effective_end_ts, is_current
FROM nessie."default".source_sales_scd2_iceberg
WHERE id = 5042
ORDER BY effective_start_ts
```

**Trino — time travel:**

```sql
SELECT *
FROM nessie."default".source_sales_scd2_iceberg
FOR TIMESTAMP AS OF TIMESTAMP '2024-02-01 12:00:00'
WHERE is_current = true
```

**Trino — cross-source federated query:**

```sql
SELECT
    i.category,
    COUNT(*)              AS lakehouse_records,
    SUM(i.price)          AS total_revenue
FROM nessie."default".source_sales_scd2_iceberg i
WHERE i.is_current = true
  AND i.is_deleted = false
GROUP BY i.category
ORDER BY total_revenue DESC
```

---

## Future Work

The following additions would bring this platform to full enterprise readiness:

- **Schema Registry** (Apicurio or Confluent): enforces schema compatibility contracts on every Kafka topic, preventing silent downstream breakage on schema evolution.
- **Data Catalog and Lineage** (OpenMetadata or DataHub): auto-discovery of Iceberg tables from Nessie, lineage built from Airflow DAG metadata, column-level impact analysis.
- **Monitoring** (Prometheus + Grafana): Kafka consumer lag, Flink checkpoint duration, Spark job runtime, and Pinot segment freshness in a unified dashboard.
- **Multi-node deployment**: Kafka, Flink, Pinot, and Spark all support horizontal scaling. Iceberg tables are designed for distributed reads from the first write.
- **CeleryExecutor for Airflow**: the current setup uses LocalExecutor, which is appropriate for a lab but does not support parallel task execution at scale.

---

## References

- [Debezium Postgres Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Apache Flink SQL Connectors](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/overview/)
- [Apache Iceberg Table Spec](https://iceberg.apache.org/spec/)
- [Project Nessie REST API v2](https://projectnessie.org/develop/rest/)
- [Project Nessie CLI](https://projectnessie.org/nessie-0-105-6-0-105-7/cli/)
- [Trino](https://trino.io/docs/current/release/release-443.html)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Apache Pinot Upsert Tables](https://docs.pinot.apache.org/basics/data-import/upsert)
