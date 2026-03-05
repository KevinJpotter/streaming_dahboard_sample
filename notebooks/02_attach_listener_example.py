# Databricks notebook source
# MAGIC %md
# MAGIC # Attach Monitoring Listener to a Streaming Job
# MAGIC
# MAGIC Demonstrates how to attach the `MonitoringQueryListener` to any
# MAGIC Structured Streaming query for automatic metric collection.
# MAGIC
# MAGIC **Prerequisites:** Run `01_setup_monitoring_tables` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "monitoring", "Monitoring Schema")
dbutils.widgets.dropdown("environment", "prod", ["dev", "stage", "prod"], "Environment")
dbutils.widgets.text("job_id", "", "Job ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
environment = dbutils.widgets.get("environment")
job_id = dbutils.widgets.get("job_id") or None

# Monitoring configuration dict — passed to listener and writer
try:
    workspace_id = spark.conf.get("spark.databricks.workspaceId")
except Exception:
    workspace_id = None

monitoring_config = {
    "catalog": catalog,
    "schema": schema,
    "environment": environment,
    "workspace_id": workspace_id,
    "job_id": job_id,
    "sla_latency_critical_ms": 60_000,   # 60 second SLA
    "writer_batch_size": 50,
    "writer_flush_interval_seconds": 10,
    "writer_queue_max_size": 10_000,
}

print(f"Config: {monitoring_config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Initialize the Metrics Writer and Listener

# COMMAND ----------

# Add the project root to the Python path
import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/streaming_dashboard")

from listener import MonitoringQueryListener, MetricsWriter
from listener.metrics_writer import InfrastructurePoller

# Start the async writer (daemon thread)
writer = MetricsWriter(spark, monitoring_config)
writer.start()

# Create the listener
listener = MonitoringQueryListener(writer, monitoring_config)

# Register the listener with SparkSession
spark.streams.addListener(listener)

print("Monitoring listener attached to SparkSession")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. (Optional) Register a Data Quality Callback
# MAGIC
# MAGIC Quality callbacks are user-defined functions that run per batch
# MAGIC and return quality metrics to be written to `data_quality_metrics`.

# COMMAND ----------

def my_quality_callback(query_id, batch_id, progress_json):
    """
    Example quality callback.
    In production, this would inspect the actual micro-batch DataFrame.
    """
    return {
        "null_rate_by_column": {"col_a": 0.01, "col_b": 0.0},
        "duplicate_rate": 0.005,
        "dropped_record_count": 0,
        "schema_change_detected": False,
        "late_record_percentage": 0.02,
    }

listener.register_quality_callback(my_quality_callback)
print("Quality callback registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Optional) Set Business Tags for a Stream

# COMMAND ----------

# After starting a stream, tag it with business metadata:
# listener.set_tags(
#     query_id="<your-query-id>",
#     domain="payments",
#     owner="data-platform-team",
#     criticality="critical",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Start a Sample Streaming Query
# MAGIC
# MAGIC Replace this with your actual streaming workload.

# COMMAND ----------

# Example: rate source for testing
df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
)

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .queryName("sample_rate_stream")
    .option("checkpointLocation", f"/tmp/monitoring_demo/checkpoint")
    .toTable(f"{catalog}.default.monitoring_demo_sink")
)

print(f"Stream started: {query.name} (id: {query.id})")

# Tag the stream
listener.set_tags(
    query_id=str(query.id),
    domain="demo",
    owner="platform-team",
    criticality="low",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Metrics Are Being Written

# COMMAND ----------

import time
time.sleep(30)  # Wait for a few batches

display(
    spark.sql(f"""
        SELECT query_name, batch_id, timestamp,
               input_rows_per_second, processed_rows_per_second,
               batch_duration_ms
        FROM {catalog}.monitoring.streaming_metrics
        WHERE environment = '{environment}'
        ORDER BY timestamp DESC
        LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Optional) Start Infrastructure Poller

# COMMAND ----------

# Uncomment to start infrastructure polling (requires cluster API access)
# infra_config = {
#     **monitoring_config,
#     "cluster_id": spark.conf.get("spark.databricks.clusterUsageTags.clusterId", ""),
#     "databricks_host": dbutils.notebook.entry_point.getDbutils()
#         .notebook().getContext().apiUrl().getOrElse(None),
#     "databricks_token": dbutils.notebook.entry_point.getDbutils()
#         .notebook().getContext().apiToken().getOrElse(None),
#     "infra_poll_interval_seconds": 60,
# }
# poller = InfrastructurePoller(writer, infra_config)
# poller.start()
# print("Infrastructure poller started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC To stop monitoring:
# MAGIC ```python
# MAGIC spark.streams.removeListener(listener)
# MAGIC writer.stop()
# MAGIC # poller.stop()  # if started
# MAGIC ```
