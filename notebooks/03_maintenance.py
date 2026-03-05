# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring Table Maintenance
# MAGIC
# MAGIC Scheduled job for OPTIMIZE + ZORDER on monitoring tables.
# MAGIC Run daily during off-peak hours.
# MAGIC
# MAGIC **Recommended schedule:** Daily at 03:00 UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "monitoring", "Monitoring Schema")
dbutils.widgets.text("retention_days", "90", "Data Retention (days)")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
retention_days = int(dbutils.widgets.get("retention_days"))

schema = f"{catalog}.{schema_name}"
print(f"Schema: {schema}")
print(f"Retention: {retention_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE (Liquid Clustering)
# MAGIC
# MAGIC Tables use liquid clustering (`CLUSTER BY`) — no ZORDER needed.
# MAGIC `OPTIMIZE` triggers incremental clustering automatically.

# COMMAND ----------

optimize_tables = [
    f"{schema}.streaming_metrics",
    f"{schema}.stream_errors",
    f"{schema}.data_quality_metrics",
    f"{schema}.sla_latency_metrics",
    f"{schema}.infrastructure_metrics",
]

for table in optimize_tables:
    print(f"Optimizing {table}...")
    try:
        spark.sql(f"OPTIMIZE {table}")
        print(f"  Done: {table}")
    except Exception as e:
        print(f"  WARNING: {table} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Retention — Delete Old Data
# MAGIC
# MAGIC Removes data older than `retention_days` to control storage costs.

# COMMAND ----------

from datetime import datetime, timedelta

cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
print(f"Deleting data before: {cutoff_date}")

tables = [
    f"{schema}.streaming_metrics",
    f"{schema}.stream_errors",
    f"{schema}.data_quality_metrics",
    f"{schema}.sla_latency_metrics",
    f"{schema}.infrastructure_metrics",
]

for table in tables:
    print(f"  Purging {table}...")
    try:
        spark.sql(f"DELETE FROM {table} WHERE CAST(timestamp AS DATE) < '{cutoff_date}'")
        print(f"    Done")
    except Exception as e:
        print(f"    WARNING: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM — Reclaim Storage

# COMMAND ----------

for table in tables:
    print(f"  Vacuuming {table}...")
    try:
        spark.sql(f"VACUUM {table} RETAIN {retention_days} HOURS")
        print(f"    Done")
    except Exception as e:
        print(f"    WARNING: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\nTable sizes:")
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0].cnt
    print(f"  {table}: {count:,} rows")

print("\nMaintenance complete.")
