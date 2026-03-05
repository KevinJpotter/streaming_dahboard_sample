-- ============================================================
-- sla_latency_metrics: latency percentiles and SLA tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS ${catalog}.monitoring.sla_latency_metrics (
  query_id          STRING      NOT NULL  COMMENT 'Streaming query identifier',
  query_name        STRING                COMMENT 'Human-readable stream name',
  batch_id          BIGINT      NOT NULL  COMMENT 'Micro-batch sequence number',
  timestamp         TIMESTAMP   NOT NULL  COMMENT 'Batch completion timestamp',
  event_time_min    TIMESTAMP             COMMENT 'Earliest event time in batch',
  event_time_max    TIMESTAMP             COMMENT 'Latest event time in batch',
  processing_time   TIMESTAMP             COMMENT 'Batch processing wall-clock time',
  latency_ms        BIGINT                COMMENT 'processing_time - event_time_max in ms',
  latency_p50       BIGINT                COMMENT '50th percentile latency (ms)',
  latency_p90       BIGINT                COMMENT '90th percentile latency (ms)',
  latency_p99       BIGINT                COMMENT '99th percentile latency (ms)',
  max_latency       BIGINT                COMMENT 'Maximum observed latency (ms)',
  sla_breach_flag   BOOLEAN               COMMENT 'True if latency exceeds SLA threshold',
  sla_threshold_ms  BIGINT                COMMENT 'SLA threshold applied',
  environment       STRING      NOT NULL  COMMENT 'dev | stage | prod',
  workspace_id      STRING                COMMENT 'Databricks workspace identifier',
  ingestion_ts      TIMESTAMP   NOT NULL  DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, sla_breach_flag, timestamp)
COMMENT 'SLA latency percentiles and breach tracking';
