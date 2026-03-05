# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Streaming Monitoring Tables
# MAGIC
# MAGIC Creates the monitoring schema and all 5 Delta tables in Unity Catalog.
# MAGIC Run this notebook once per environment to initialize the monitoring infrastructure.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name (default: `main`)
# MAGIC - `environment`: Target environment (default: `prod`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.dropdown("environment", "prod", ["dev", "stage", "prod"], "Environment")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Catalog: {catalog}")
print(f"Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.monitoring
    COMMENT 'Streaming monitoring tables for structured streaming observability'
""")
print(f"Schema {catalog}.monitoring ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

# --- streaming_metrics (event log format) ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.monitoring.streaming_metrics (
  id                          STRING        NOT NULL,
  sequence                    STRING,
  origin                      STRING,
  timestamp                   TIMESTAMP     NOT NULL,
  message                     STRING,
  level                       STRING        NOT NULL,
  event_type                  STRING        NOT NULL DEFAULT 'stream_progress',
  maturity_level              STRING,
  details                     STRING        NOT NULL,
  environment                 STRING        NOT NULL,
  workspace_id                STRING,
  domain                      STRING,
  owner                       STRING,
  criticality                 STRING,
  ingestion_ts                TIMESTAMP     NOT NULL DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (event_type, timestamp)
COMMENT 'Streaming metrics in event log format — details column matches stream_progress schema'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false'
)
""")
print("Created: streaming_metrics")

# COMMAND ----------

# --- stream_errors ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.monitoring.stream_errors (
  query_id              STRING        NOT NULL,
  query_name            STRING,
  batch_id              BIGINT,
  timestamp             TIMESTAMP     NOT NULL,
  exception_type        STRING,
  exception_message     STRING,
  termination_reason    STRING,
  restart_count         INT,
  stack_trace           STRING,
  environment           STRING        NOT NULL,
  workspace_id          STRING,
  ingestion_ts          TIMESTAMP     NOT NULL DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, timestamp)
COMMENT 'Stream error and termination events'
""")
print("Created: stream_errors")

# COMMAND ----------

# --- data_quality_metrics ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.monitoring.data_quality_metrics (
  query_id                STRING              NOT NULL,
  query_name              STRING,
  batch_id                BIGINT              NOT NULL,
  timestamp               TIMESTAMP           NOT NULL,
  null_rate_by_column     MAP<STRING, DOUBLE>,
  duplicate_rate          DOUBLE,
  dropped_record_count    BIGINT,
  schema_change_detected  BOOLEAN,
  late_record_percentage  DOUBLE,
  environment             STRING              NOT NULL,
  workspace_id            STRING,
  ingestion_ts            TIMESTAMP           NOT NULL DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, timestamp)
COMMENT 'Per-batch data quality metrics'
""")
print("Created: data_quality_metrics")

# COMMAND ----------

# --- sla_latency_metrics ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.monitoring.sla_latency_metrics (
  query_id          STRING      NOT NULL,
  query_name        STRING,
  batch_id          BIGINT      NOT NULL,
  timestamp         TIMESTAMP   NOT NULL,
  event_time_min    TIMESTAMP,
  event_time_max    TIMESTAMP,
  processing_time   TIMESTAMP,
  latency_ms        BIGINT,
  latency_p50       BIGINT,
  latency_p90       BIGINT,
  latency_p99       BIGINT,
  max_latency       BIGINT,
  sla_breach_flag   BOOLEAN,
  sla_threshold_ms  BIGINT,
  environment       STRING      NOT NULL,
  workspace_id      STRING,
  ingestion_ts      TIMESTAMP   NOT NULL DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, sla_breach_flag, timestamp)
COMMENT 'SLA latency percentiles and breach tracking'
""")
print("Created: sla_latency_metrics")

# COMMAND ----------

# --- infrastructure_metrics ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.monitoring.infrastructure_metrics (
  cluster_id           STRING      NOT NULL,
  timestamp            TIMESTAMP   NOT NULL,
  cpu_usage_percent    DOUBLE,
  memory_usage_percent DOUBLE,
  executor_count       INT,
  autoscaling_event    STRING,
  dbu_consumption      DOUBLE,
  driver_node_type     STRING,
  worker_node_type     STRING,
  environment          STRING      NOT NULL,
  workspace_id         STRING,
  ingestion_ts         TIMESTAMP   NOT NULL DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (cluster_id, timestamp)
COMMENT 'Cluster infrastructure metrics'
""")
print("Created: infrastructure_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.monitoring").collect()
print(f"\nTables in {catalog}.monitoring:")
for t in tables:
    print(f"  - {t.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC All monitoring tables are created and ready. Next:
# MAGIC 1. Attach the `MonitoringQueryListener` to your streaming jobs (see `02_attach_listener_example`)
# MAGIC 2. Deploy the Lakeview dashboard (see `deploy/deploy_dashboard.py`)
