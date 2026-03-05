# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Monitoring — Lakeflow Declarative Pipeline
# MAGIC
# MAGIC Replaces the custom `StreamingQueryListener` + `MetricsWriter` ETL with a
# MAGIC fully declarative pipeline that reads from a source pipeline's event log
# MAGIC and materializes all monitoring tables.
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Source DLT Pipeline
# MAGIC       ↓ (event_log)
# MAGIC This Pipeline (Lakeflow Declarative)
# MAGIC       ↓
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │  Bronze: raw_events (streaming table)        │
# MAGIC │      ↓                                       │
# MAGIC │  Silver: streaming_metrics                   │
# MAGIC │          stream_errors                       │
# MAGIC │          sla_latency_metrics                 │
# MAGIC │          data_quality_metrics                │
# MAGIC │      ↓                                       │
# MAGIC │  Gold:   mv_stream_overview                  │
# MAGIC │          mv_throughput_trends                 │
# MAGIC │          mv_latency_sla                      │
# MAGIC │          mv_backpressure                     │
# MAGIC │          mv_state_store_health               │
# MAGIC │          mv_data_quality                     │
# MAGIC │          mv_infrastructure_cost              │
# MAGIC └─────────────────────────────────────────────┘
# MAGIC       ↓
# MAGIC Lakeview Dashboard → Alerting
# MAGIC ```
# MAGIC
# MAGIC ## Pipeline Configuration
# MAGIC Set these in the pipeline settings under **Configuration**:
# MAGIC
# MAGIC | Key | Example | Description |
# MAGIC |-----|---------|-------------|
# MAGIC | `source_event_log_table` | `main.source_schema.event_log` | Fully qualified event log table |
# MAGIC | `monitoring_environment` | `prod` | Environment tag (dev/stage/prod) |
# MAGIC | `sla_latency_critical_ms` | `60000` | SLA threshold in ms |
# MAGIC | `time_range_days` | `7` | Lookback window for gold MVs |

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Parameters

# COMMAND ----------

SOURCE_EVENT_LOG = spark.conf.get("source_event_log_table", "main.default.event_log")
ENVIRONMENT = spark.conf.get("monitoring_environment", "prod")
SLA_THRESHOLD_MS = int(spark.conf.get("sla_latency_critical_ms", "60000"))
TIME_RANGE_DAYS = int(spark.conf.get("time_range_days", "7"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Event Log Ingestion

# COMMAND ----------

@dlt.table(
    comment="Raw events streamed from the source pipeline event log",
    table_properties={"quality": "bronze"},
)
@dlt.expect("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IS NOT NULL")
def raw_events():
    return (
        spark.readStream
        .table(SOURCE_EVENT_LOG)
        .select(
            F.col("id"),
            F.col("sequence"),
            F.col("origin"),
            F.col("timestamp"),
            F.col("message"),
            F.col("level"),
            F.col("event_type"),
            F.col("maturity_level"),
            F.col("details"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Streaming Metrics
# MAGIC Parses `stream_progress` events into the event-log-format monitoring table.

# COMMAND ----------

@dlt.table(
    comment="Per-micro-batch streaming metrics in event log format — details matches stream_progress schema",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_details", "details IS NOT NULL")
@dlt.expect("is_stream_progress", "event_type = 'stream_progress'")
def streaming_metrics():
    return (
        dlt.read_stream("raw_events")
        .filter(F.col("event_type") == "stream_progress")
        .select(
            F.col("id"),
            F.col("sequence"),
            F.col("origin"),
            F.col("timestamp"),
            F.col("message"),
            F.col("level"),
            F.col("event_type"),
            F.col("maturity_level"),
            F.col("details"),
            F.lit(ENVIRONMENT).alias("environment"),
            F.get_json_object(F.col("origin"), "$.workspace_id").alias("workspace_id"),
            F.get_json_object(F.col("origin"), "$.flow_name").alias("domain"),
            F.lit(None).cast(StringType()).alias("owner"),
            F.lit(None).cast(StringType()).alias("criticality"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Stream Errors
# MAGIC Captures pipeline error and termination events.

# COMMAND ----------

@dlt.table(
    comment="Stream error and termination events from pipeline event log",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("is_error_event", "level IN ('ERROR', 'WARN')")
def stream_errors():
    return (
        dlt.read_stream("raw_events")
        .filter(F.col("level").isin("ERROR", "WARN"))
        .select(
            F.get_json_object(F.col("origin"), "$.flow_id").alias("query_id"),
            F.get_json_object(F.col("origin"), "$.flow_name").alias("query_name"),
            F.get_json_object(F.col("details"), "$.error.batch_id")
             .cast(LongType()).alias("batch_id"),
            F.col("timestamp"),
            F.get_json_object(F.col("details"), "$.error.exception_type")
             .alias("exception_type"),
            F.get_json_object(F.col("details"), "$.error.message")
             .alias("exception_message"),
            F.get_json_object(F.col("details"), "$.error.termination_reason")
             .alias("termination_reason"),
            F.lit(None).cast("INT").alias("restart_count"),
            F.get_json_object(F.col("details"), "$.error.stack_trace")
             .alias("stack_trace"),
            F.lit(ENVIRONMENT).alias("environment"),
            F.get_json_object(F.col("origin"), "$.workspace_id").alias("workspace_id"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: SLA Latency Metrics
# MAGIC Computes latency from `stream_progress` event times and flags SLA breaches.

# COMMAND ----------

@dlt.table(
    comment="SLA latency percentiles and breach tracking derived from stream_progress events",
    table_properties={"quality": "silver"},
)
@dlt.expect("has_event_time", "event_time_max IS NOT NULL")
def sla_latency_metrics():
    progress = (
        dlt.read_stream("raw_events")
        .filter(F.col("event_type") == "stream_progress")
    )

    return (
        progress
        .select(
            F.get_json_object(F.col("details"), "$.stream_progress.id")
             .alias("query_id"),
            F.get_json_object(F.col("details"), "$.stream_progress.name")
             .alias("query_name"),
            F.get_json_object(F.col("details"), "$.stream_progress.batchId")
             .cast(LongType()).alias("batch_id"),
            F.col("timestamp"),
            F.to_timestamp(
                F.get_json_object(F.col("details"), "$.stream_progress.eventTime.min")
            ).alias("event_time_min"),
            F.to_timestamp(
                F.get_json_object(F.col("details"), "$.stream_progress.eventTime.max")
            ).alias("event_time_max"),
            F.col("timestamp").alias("processing_time"),
        )
        .withColumn(
            "latency_ms",
            (
                F.unix_timestamp(F.col("processing_time"))
                - F.unix_timestamp(F.col("event_time_max"))
            ).cast(LongType()) * 1000,
        )
        .withColumn("latency_p50", F.col("latency_ms"))
        .withColumn("latency_p90", F.col("latency_ms"))
        .withColumn("latency_p99", F.col("latency_ms"))
        .withColumn("max_latency", F.col("latency_ms"))
        .withColumn(
            "sla_breach_flag",
            F.when(F.col("latency_ms") > F.lit(SLA_THRESHOLD_MS), True)
             .otherwise(False),
        )
        .withColumn("sla_threshold_ms", F.lit(SLA_THRESHOLD_MS).cast(LongType()))
        .withColumn("environment", F.lit(ENVIRONMENT))
        .withColumn(
            "workspace_id",
            F.get_json_object(F.col("origin"), "$.workspace_id"),
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .drop("origin")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Data Quality Metrics
# MAGIC Extracts `observedMetrics` from `stream_progress` for quality tracking.

# COMMAND ----------

@dlt.table(
    comment="Per-batch data quality metrics derived from stream_progress observedMetrics",
    table_properties={"quality": "silver"},
)
def data_quality_metrics():
    return (
        dlt.read_stream("raw_events")
        .filter(F.col("event_type") == "stream_progress")
        .filter(
            F.get_json_object(F.col("details"), "$.stream_progress.observedMetrics")
            .isNotNull()
        )
        .select(
            F.get_json_object(F.col("details"), "$.stream_progress.id")
             .alias("query_id"),
            F.get_json_object(F.col("details"), "$.stream_progress.name")
             .alias("query_name"),
            F.get_json_object(F.col("details"), "$.stream_progress.batchId")
             .cast(LongType()).alias("batch_id"),
            F.col("timestamp"),
            F.lit(None).cast(MapType(StringType(), DoubleType()))
             .alias("null_rate_by_column"),
            F.get_json_object(
                F.col("details"),
                "$.stream_progress.observedMetrics.quality.duplicate_rate",
            ).cast(DoubleType()).alias("duplicate_rate"),
            F.get_json_object(
                F.col("details"),
                "$.stream_progress.observedMetrics.quality.dropped_record_count",
            ).cast(LongType()).alias("dropped_record_count"),
            F.lit(False).alias("schema_change_detected"),
            F.get_json_object(
                F.col("details"),
                "$.stream_progress.observedMetrics.quality.late_record_percentage",
            ).cast(DoubleType()).alias("late_record_percentage"),
            F.lit(ENVIRONMENT).alias("environment"),
            F.get_json_object(F.col("origin"), "$.workspace_id")
             .alias("workspace_id"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Infrastructure Metrics
# MAGIC Reads from `system.billing.usage` and cluster events for cost/infra data.
# MAGIC This table uses a batch (non-streaming) source since billing data is not
# MAGIC available in the event log.

# COMMAND ----------

@dlt.table(
    comment="Cluster infrastructure metrics from system billing tables",
    table_properties={"quality": "silver"},
)
def infrastructure_metrics():
    return (
        spark.sql(f"""
            SELECT
                cluster_id,
                usage_date                          AS timestamp,
                NULL                                AS cpu_usage_percent,
                NULL                                AS memory_usage_percent,
                NULL                                AS executor_count,
                NULL                                AS autoscaling_event,
                ROUND(usage_quantity, 2)             AS dbu_consumption,
                NULL                                AS driver_node_type,
                NULL                                AS worker_node_type,
                '{ENVIRONMENT}'                     AS environment,
                workspace_id,
                current_timestamp()                 AS ingestion_ts
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
              AND usage_metadata.cluster_id IS NOT NULL
        """)
        .withColumnRenamed("usage_metadata.cluster_id", "cluster_id")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Layer: Materialized Views for Dashboard
# MAGIC
# MAGIC Each MV corresponds to one Lakeview dashboard page.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 1: Stream Overview

# COMMAND ----------

@dlt.table(
    comment="Stream overview: status, uptime, last batch, SLA breaches",
    table_properties={"quality": "gold"},
)
def mv_stream_overview():
    return spark.sql(f"""
        WITH latest_batch AS (
            SELECT
                details:stream_progress.id::STRING            AS query_id,
                details:stream_progress.name::STRING          AS query_name,
                MAX(timestamp)                                AS last_batch_ts,
                MAX(details:stream_progress.batchId::BIGINT)  AS last_batch_id,
                MIN(timestamp)                                AS first_batch_ts
            FROM LIVE.streaming_metrics
            WHERE environment = '{ENVIRONMENT}'
            GROUP BY
                details:stream_progress.id::STRING,
                details:stream_progress.name::STRING
        ),
        error_status AS (
            SELECT query_id, MAX(timestamp) AS last_error_ts
            FROM LIVE.stream_errors
            WHERE environment = '{ENVIRONMENT}'
            GROUP BY query_id
        ),
        sla_breaches AS (
            SELECT query_id, COUNT(*) AS breach_count
            FROM LIVE.sla_latency_metrics
            WHERE environment = '{ENVIRONMENT}'
              AND sla_breach_flag = true
            GROUP BY query_id
        )
        SELECT
            lb.query_id,
            lb.query_name,
            CASE
                WHEN es.last_error_ts > lb.last_batch_ts THEN 'ERROR'
                WHEN lb.last_batch_ts < current_timestamp() - INTERVAL 10 MINUTE THEN 'INACTIVE'
                ELSE 'ACTIVE'
            END                                          AS stream_status,
            lb.last_batch_ts,
            lb.last_batch_id,
            lb.first_batch_ts,
            CAST(
                (unix_timestamp(lb.last_batch_ts) - unix_timestamp(lb.first_batch_ts)) / 3600.0
                AS DECIMAL(10,1)
            )                                            AS uptime_hours,
            COALESCE(sb.breach_count, 0)                 AS sla_breach_count,
            COALESCE(sb.breach_count, 0) > 0             AS has_sla_breaches,
            es.last_error_ts
        FROM latest_batch lb
        LEFT JOIN error_status es ON lb.query_id = es.query_id
        LEFT JOIN sla_breaches sb ON lb.query_id = sb.query_id
        ORDER BY lb.query_name
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 2: Throughput Trends

# COMMAND ----------

@dlt.table(
    comment="Throughput trends: input/processed rows/sec, batch duration, watermark delay",
    table_properties={"quality": "gold"},
)
def mv_throughput_trends():
    return spark.sql(f"""
        SELECT
            details:stream_progress.name::STRING                                      AS query_name,
            window(timestamp, '1 minute').start                                       AS time_bucket,
            ROUND(AVG(details:stream_progress.inputRowsPerSecond::DOUBLE), 2)         AS avg_input_rows_per_sec,
            ROUND(AVG(details:stream_progress.processedRowsPerSecond::DOUBLE), 2)     AS avg_processed_rows_per_sec,
            ROUND(AVG(details:stream_progress.batchDuration::BIGINT), 0)              AS avg_batch_duration_ms,
            MAX(details:stream_progress.batchDuration::BIGINT)                        AS max_batch_duration_ms,
            SUM(details:stream_progress.numInputRows::BIGINT)                         AS total_input_rows,
            ROUND(AVG(
                unix_timestamp(timestamp) -
                unix_timestamp(details:stream_progress.eventTime.watermark::TIMESTAMP)
            ), 0)                                                                     AS avg_watermark_delay_sec
        FROM LIVE.streaming_metrics
        WHERE environment = '{ENVIRONMENT}'
          AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
        GROUP BY
            details:stream_progress.name::STRING,
            window(timestamp, '1 minute').start
        ORDER BY query_name, time_bucket
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 3: Latency & SLA

# COMMAND ----------

@dlt.table(
    comment="Latency percentile trends and SLA breach rates",
    table_properties={"quality": "gold"},
)
def mv_latency_sla():
    return spark.sql(f"""
        WITH latency_stats AS (
            SELECT
                query_name,
                window(timestamp, '5 minutes').start     AS time_bucket,
                PERCENTILE_APPROX(latency_ms, 0.50)      AS p50_latency_ms,
                PERCENTILE_APPROX(latency_ms, 0.90)      AS p90_latency_ms,
                PERCENTILE_APPROX(latency_ms, 0.99)      AS p99_latency_ms,
                MAX(max_latency)                          AS max_latency_ms,
                COUNT(*)                                  AS batch_count,
                SUM(CASE WHEN sla_breach_flag THEN 1 ELSE 0 END) AS breach_count,
                sla_threshold_ms
            FROM LIVE.sla_latency_metrics
            WHERE environment = '{ENVIRONMENT}'
              AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
              AND latency_ms IS NOT NULL
            GROUP BY query_name, window(timestamp, '5 minutes').start, sla_threshold_ms
        )
        SELECT
            query_name,
            time_bucket,
            p50_latency_ms,
            p90_latency_ms,
            p99_latency_ms,
            max_latency_ms,
            breach_count,
            batch_count,
            ROUND(100.0 * breach_count / batch_count, 2) AS breach_percentage,
            sla_threshold_ms
        FROM latency_stats
        ORDER BY query_name, time_bucket
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 4: Backpressure Detection

# COMMAND ----------

@dlt.table(
    comment="Backpressure detection: input/processed ratio with 5-batch sustained window",
    table_properties={"quality": "gold"},
)
def mv_backpressure():
    return spark.sql(f"""
        WITH batch_ratios AS (
            SELECT
                details:stream_progress.id::STRING                         AS query_id,
                details:stream_progress.name::STRING                       AS query_name,
                details:stream_progress.batchId::BIGINT                    AS batch_id,
                timestamp,
                details:stream_progress.inputRowsPerSecond::DOUBLE         AS input_rows_per_second,
                details:stream_progress.processedRowsPerSecond::DOUBLE     AS processed_rows_per_second,
                CASE
                    WHEN details:stream_progress.processedRowsPerSecond::DOUBLE > 0
                    THEN ROUND(
                        details:stream_progress.inputRowsPerSecond::DOUBLE /
                        details:stream_progress.processedRowsPerSecond::DOUBLE, 3)
                    ELSE NULL
                END                                                        AS backpressure_ratio,
                CASE
                    WHEN details:stream_progress.inputRowsPerSecond::DOUBLE >
                         details:stream_progress.processedRowsPerSecond::DOUBLE THEN 1
                    ELSE 0
                END                                                        AS is_backpressured
            FROM LIVE.streaming_metrics
            WHERE environment = '{ENVIRONMENT}'
              AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
        ),
        windowed AS (
            SELECT *,
                SUM(is_backpressured) OVER (
                    PARTITION BY query_id
                    ORDER BY batch_id
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sustained_count
            FROM batch_ratios
        )
        SELECT
            query_name, batch_id, timestamp,
            input_rows_per_second, processed_rows_per_second,
            backpressure_ratio, sustained_count,
            CASE
                WHEN sustained_count >= 5 THEN 'CRITICAL'
                WHEN sustained_count >= 3 THEN 'WARNING'
                ELSE 'OK'
            END AS backpressure_status
        FROM windowed
        ORDER BY query_name, batch_id DESC
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 5: State Store Health

# COMMAND ----------

@dlt.table(
    comment="State store health: memory trend, row growth, growth rate",
    table_properties={"quality": "gold"},
)
def mv_state_store_health():
    return spark.sql(f"""
        WITH state_data AS (
            SELECT
                details:stream_progress.name::STRING                  AS query_name,
                details:stream_progress.id::STRING                    AS query_id,
                timestamp,
                details:stream_progress.batchId::BIGINT               AS batch_id,
                AGGREGATE(
                    from_json(
                        details:stream_progress.stateOperators,
                        'ARRAY<STRUCT<memoryUsedBytes:BIGINT, numRowsTotal:BIGINT>>'
                    ),
                    CAST(0 AS BIGINT),
                    (acc, x) -> acc + COALESCE(x.memoryUsedBytes, 0)
                ) AS state_operator_memory_bytes,
                AGGREGATE(
                    from_json(
                        details:stream_progress.stateOperators,
                        'ARRAY<STRUCT<memoryUsedBytes:BIGINT, numRowsTotal:BIGINT>>'
                    ),
                    CAST(0 AS BIGINT),
                    (acc, x) -> acc + COALESCE(x.numRowsTotal, 0)
                ) AS state_rows_total
            FROM LIVE.streaming_metrics
            WHERE environment = '{ENVIRONMENT}'
              AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
        ),
        with_lag AS (
            SELECT
                query_name, timestamp, batch_id,
                state_operator_memory_bytes,
                state_rows_total,
                ROUND(state_operator_memory_bytes / (1024.0 * 1024.0), 2) AS state_memory_mb,
                LAG(state_operator_memory_bytes) OVER (
                    PARTITION BY query_id ORDER BY batch_id
                ) AS prev_memory_bytes
            FROM state_data
            WHERE state_operator_memory_bytes > 0
        )
        SELECT
            query_name, timestamp, batch_id,
            state_memory_mb, state_rows_total,
            CASE
                WHEN prev_memory_bytes > 0
                THEN ROUND(100.0 * (state_operator_memory_bytes - prev_memory_bytes) / prev_memory_bytes, 2)
                ELSE 0
            END AS memory_growth_pct
        FROM with_lag
        ORDER BY query_name, batch_id
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 6: Data Quality

# COMMAND ----------

@dlt.table(
    comment="Data quality trends: duplicate rate, dropped records, late records, schema changes",
    table_properties={"quality": "gold"},
)
def mv_data_quality():
    return spark.sql(f"""
        SELECT
            query_name,
            window(timestamp, '1 hour').start               AS time_bucket,
            ROUND(AVG(duplicate_rate) * 100, 2)              AS avg_duplicate_rate_pct,
            SUM(dropped_record_count)                        AS total_dropped_records,
            ROUND(AVG(late_record_percentage) * 100, 2)      AS avg_late_record_pct,
            SUM(CASE WHEN schema_change_detected THEN 1 ELSE 0 END) AS schema_change_count,
            COUNT(*)                                         AS batch_count
        FROM LIVE.data_quality_metrics
        WHERE environment = '{ENVIRONMENT}'
          AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
        GROUP BY query_name, window(timestamp, '1 hour').start
        ORDER BY query_name, time_bucket
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 7: Infrastructure & Cost

# COMMAND ----------

@dlt.table(
    comment="Infrastructure and cost: CPU/memory, executor count, DBU consumption",
    table_properties={"quality": "gold"},
)
def mv_infrastructure_cost():
    return spark.sql(f"""
        SELECT
            cluster_id,
            window(timestamp, '1 hour').start               AS time_bucket,
            ROUND(AVG(cpu_usage_percent), 1)                 AS avg_cpu_pct,
            MAX(cpu_usage_percent)                           AS max_cpu_pct,
            ROUND(AVG(memory_usage_percent), 1)              AS avg_memory_pct,
            MAX(memory_usage_percent)                        AS max_memory_pct,
            ROUND(AVG(executor_count), 0)                    AS avg_executor_count,
            MAX(executor_count)                              AS max_executor_count,
            ROUND(SUM(COALESCE(dbu_consumption, 0)), 2)      AS total_dbu,
            SUM(CASE WHEN autoscaling_event = 'SCALE_UP' THEN 1 ELSE 0 END)   AS scale_up_count,
            SUM(CASE WHEN autoscaling_event = 'SCALE_DOWN' THEN 1 ELSE 0 END) AS scale_down_count
        FROM LIVE.infrastructure_metrics
        WHERE environment = '{ENVIRONMENT}'
          AND CAST(timestamp AS DATE) >= current_date() - INTERVAL {TIME_RANGE_DAYS} DAY
        GROUP BY cluster_id, window(timestamp, '1 hour').start
        ORDER BY cluster_id, time_bucket
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Deployment
# MAGIC
# MAGIC Create the pipeline via UI or CLI:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "streaming_monitoring_pipeline",
# MAGIC   "target": "monitoring",
# MAGIC   "catalog": "main",
# MAGIC   "libraries": [
# MAGIC     {"notebook": {"path": "/Repos/<repo>/streaming_dashboard/notebooks/04_dlt_monitoring_pipeline"}}
# MAGIC   ],
# MAGIC   "configuration": {
# MAGIC     "source_event_log_table": "main.source_schema.event_log",
# MAGIC     "monitoring_environment": "prod",
# MAGIC     "sla_latency_critical_ms": "60000",
# MAGIC     "time_range_days": "7"
# MAGIC   },
# MAGIC   "continuous": true,
# MAGIC   "channel": "CURRENT",
# MAGIC   "development": false
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Or via Databricks CLI:
# MAGIC ```bash
# MAGIC databricks pipelines create --json @pipeline_config.json
# MAGIC ```
