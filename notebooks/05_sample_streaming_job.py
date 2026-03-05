# Databricks notebook source
# MAGIC %md
# MAGIC # Sample Streaming Job — Listener Integration Test
# MAGIC
# MAGIC Generates synthetic IoT sensor data into a Unity Catalog volume (`landing_zone`),
# MAGIC then runs Structured Streaming queries with `availableNow` trigger
# MAGIC (serverless-compatible) to exercise the full `MonitoringQueryListener` pipeline.
# MAGIC
# MAGIC Each iteration: generate a data batch, run ingest stream, run aggregation stream.
# MAGIC This produces multiple micro-batch progress events for the listener to capture.
# MAGIC
# MAGIC **Prerequisites:** Run `01_setup_monitoring_tables` first to create the monitoring schema and tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_az5k2q_catalog", "Catalog")
dbutils.widgets.text("schema", "monitoring", "Monitoring Schema")
dbutils.widgets.dropdown("environment", "dev", ["dev", "stage", "prod"], "Environment")
dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("num_batches", "10", "Number of data batches to generate")
dbutils.widgets.text("rows_per_batch", "500", "Rows per batch")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
environment = dbutils.widgets.get("environment")
job_id = dbutils.widgets.get("job_id") or None
num_batches = int(dbutils.widgets.get("num_batches"))
rows_per_batch = int(dbutils.widgets.get("rows_per_batch"))

volume_path = f"/Volumes/{catalog}/default/landing_zone"
checkpoint_base = f"/Volumes/{catalog}/default/landing_zone/_checkpoints"
bronze_table = f"{catalog}.default.sensor_bronze"
gold_table = f"{catalog}.default.sensor_agg_gold"

print(f"Catalog:        {catalog}")
print(f"Schema:         {schema}")
print(f"Environment:    {environment}")
print(f"Volume path:    {volume_path}")
print(f"Batches:        {num_batches} x {rows_per_batch} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Volume and Target Tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.default")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.default.landing_zone
    COMMENT 'Landing zone for sample streaming data'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        sensor_id       STRING      NOT NULL,
        temperature     DOUBLE,
        humidity        DOUBLE,
        pressure        DOUBLE,
        event_time      TIMESTAMP   NOT NULL,
        region          STRING,
        facility        STRING,
        ingestion_ts    TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (region, event_time)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
        window_start    TIMESTAMP   NOT NULL,
        window_end      TIMESTAMP   NOT NULL,
        region          STRING      NOT NULL,
        sensor_count    BIGINT,
        avg_temperature DOUBLE,
        max_temperature DOUBLE,
        min_temperature DOUBLE,
        avg_humidity    DOUBLE,
        record_count    BIGINT
    )
    USING DELTA
    CLUSTER BY (region, window_start)
""")

print("Volume and tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Set Up Monitoring Listener

# COMMAND ----------

import sys, os

# Add bundle files root to path so the listener package is importable.
_nb_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
_project_root = os.path.dirname(_nb_dir)

for candidate in [
    _project_root,
    "/Workspace" + _project_root,
]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from listener import MonitoringQueryListener, MetricsWriter

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
    "sla_latency_critical_ms": 60_000,
    "writer_batch_size": 5,
    "writer_flush_interval_seconds": 3,
    "writer_queue_max_size": 10_000,
}

writer = MetricsWriter(spark, monitoring_config)
writer.start()

listener = MonitoringQueryListener(writer, monitoring_config)

def quality_callback(query_id, batch_id, progress_json):
    import random
    return {
        "null_rate_by_column": {"temperature": round(random.uniform(0, 0.03), 4),
                                "humidity": round(random.uniform(0, 0.02), 4)},
        "duplicate_rate": round(random.uniform(0, 0.01), 4),
        "dropped_record_count": random.randint(0, 3),
        "schema_change_detected": False,
        "late_record_percentage": round(random.uniform(0, 0.05), 4),
    }

listener.register_quality_callback(quality_callback)

spark.streams.addListener(listener)
print("MonitoringQueryListener attached to SparkSession.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Generation Helpers

# COMMAND ----------

import json
import random
import time
from datetime import datetime, timedelta

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
FACILITIES = ["plant-A", "plant-B", "warehouse-C", "lab-D"]
SENSOR_IDS = [f"sensor-{i:04d}" for i in range(1, 51)]


def generate_batch(batch_num, rows):
    """Generate a list of sensor reading dicts with realistic event times."""
    records = []
    base_time = datetime.utcnow() - timedelta(seconds=random.randint(0, 10))
    for _ in range(rows):
        event_time = base_time + timedelta(
            milliseconds=random.randint(-5000, 5000)
        )
        records.append({
            "sensor_id": random.choice(SENSOR_IDS),
            "temperature": round(random.gauss(72.0, 8.0), 2),
            "humidity": round(random.gauss(45.0, 12.0), 2),
            "pressure": round(random.gauss(1013.25, 5.0), 2),
            "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "region": random.choice(REGIONS),
            "facility": random.choice(FACILITIES),
        })
    return records


def write_batch_to_volume(batch_num, records):
    """Write a JSON-lines file to the landing zone volume."""
    file_name = f"sensors_{batch_num:05d}_{int(time.time() * 1000)}.json"
    file_path = f"{volume_path}/{file_name}"
    content = "\n".join(json.dumps(r) for r in records)
    dbutils.fs.put(file_path, content, overwrite=True)
    return file_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Stream Definitions

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, window, count, avg, max as spark_max,
    min as spark_min, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

sensor_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("event_time", StringType(), False),
    StructField("region", StringType(), True),
    StructField("facility", StringType(), True),
])


def run_ingest():
    """Run the Bronze ingest stream with availableNow trigger."""
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/ingest_schema")
        .schema(sensor_schema)
        .load(volume_path)
    )
    bronze_df = (
        raw_stream
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("ingestion_ts", current_timestamp())
        .select("sensor_id", "temperature", "humidity", "pressure",
                "event_time", "region", "facility", "ingestion_ts")
    )
    q = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .queryName("sensor_ingest")
        .option("checkpointLocation", f"{checkpoint_base}/sensor_ingest")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )
    q.awaitTermination()
    return q


def run_aggregation():
    """Run the Gold aggregation stream with availableNow trigger."""
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(bronze_table)
    )
    agg_df = (
        bronze_stream
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("region"),
        )
        .agg(
            count(lit(1)).alias("record_count"),
            count("sensor_id").alias("sensor_count"),
            avg("temperature").alias("avg_temperature"),
            spark_max("temperature").alias("max_temperature"),
            spark_min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("sensor_count"),
            col("avg_temperature"),
            col("max_temperature"),
            col("min_temperature"),
            col("avg_humidity"),
            col("record_count"),
        )
    )
    q = (
        agg_df.writeStream
        .format("delta")
        .outputMode("append")
        .queryName("sensor_aggregation")
        .option("checkpointLocation", f"{checkpoint_base}/sensor_aggregation")
        .trigger(availableNow=True)
        .toTable(gold_table)
    )
    q.awaitTermination()
    return q

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Data & Run Streams in Loop
# MAGIC
# MAGIC Each iteration drops a batch of JSON files, then triggers both streams
# MAGIC with `availableNow`. The listener captures progress events per iteration.

# COMMAND ----------

print(f"Running {num_batches} iterations of: generate → ingest → aggregate\n")

for i in range(1, num_batches + 1):
    # Generate and write data
    records = generate_batch(i, rows_per_batch)
    path = write_batch_to_volume(i, records)
    total_rows = i * rows_per_batch

    # Run ingest (Bronze)
    run_ingest()

    # Run aggregation (Gold)
    run_aggregation()

    print(f"  Iteration {i:3d}/{num_batches} — {rows_per_batch} rows "
          f"({total_rows:,} total) ✓")

print(f"\nAll {num_batches} iterations complete ({num_batches * rows_per_batch:,} rows total).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Flush Listener & Verify Monitoring Tables

# COMMAND ----------

# Give the writer thread time to flush remaining records
print("Waiting 15s for listener to flush...")
time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Metrics (event log format)

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        details:stream_progress.name::STRING   AS query_name,
        details:stream_progress.batchId::BIGINT AS batch_id,
        timestamp,
        ROUND(details:stream_progress.inputRowsPerSecond::DOUBLE, 1) AS input_rows_sec,
        ROUND(details:stream_progress.processedRowsPerSecond::DOUBLE, 1) AS processed_rows_sec,
        details:stream_progress.batchDuration::BIGINT AS batch_duration_ms,
        details:stream_progress.numInputRows::BIGINT AS num_input_rows
    FROM {catalog}.{schema}.streaming_metrics
    WHERE environment = '{environment}'
    ORDER BY timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SLA Latency Metrics

# COMMAND ----------

display(spark.sql(f"""
    SELECT query_name, batch_id, timestamp,
           latency_ms, sla_breach_flag, sla_threshold_ms
    FROM {catalog}.{schema}.sla_latency_metrics
    WHERE environment = '{environment}'
    ORDER BY timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Metrics

# COMMAND ----------

display(spark.sql(f"""
    SELECT query_name, batch_id, timestamp,
           duplicate_rate, dropped_record_count,
           late_record_percentage, schema_change_detected
    FROM {catalog}.{schema}.data_quality_metrics
    WHERE environment = '{environment}'
    ORDER BY timestamp DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze & Gold Row Counts

# COMMAND ----------

bronze_count = spark.table(bronze_table).count()
gold_count = spark.table(gold_table).count()
metrics_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog}.{schema}.streaming_metrics
    WHERE environment = '{environment}'
""").first().cnt

print(f"Bronze rows:            {bronze_count:,}")
print(f"Gold agg rows:          {gold_count:,}")
print(f"Monitoring metric rows: {metrics_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup
# MAGIC
# MAGIC Detach the listener and stop the writer. Uncomment the DROP statements
# MAGIC to remove test artifacts.

# COMMAND ----------

spark.streams.removeListener(listener)
writer.stop()
print("Listener and writer shut down.")

# COMMAND ----------

# Uncomment to clean up test tables and data:
# spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
# spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
# dbutils.fs.rm(volume_path, recurse=True)
# dbutils.fs.rm(checkpoint_base, recurse=True)
# print("Test artifacts removed.")
