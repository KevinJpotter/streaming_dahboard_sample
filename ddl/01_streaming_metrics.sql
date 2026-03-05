-- ============================================================
-- streaming_metrics: per-micro-batch operational metrics
-- Schema mirrors the Databricks event log table format.
-- The `details` column stores a JSON object whose
-- `stream_progress` key matches the event log's
-- stream_progress event structure.
-- ============================================================

CREATE TABLE IF NOT EXISTS ${catalog}.monitoring.streaming_metrics (
  id                          STRING        NOT NULL  COMMENT 'Unique event identifier (UUID)',
  sequence                    STRING                  COMMENT 'JSON sequence metadata for ordering',
  origin                      STRING                  COMMENT 'JSON origin metadata (workspace, pipeline, cloud)',
  timestamp                   TIMESTAMP     NOT NULL  COMMENT 'UTC time the event was recorded',
  message                     STRING                  COMMENT 'Human-readable event description',
  level                       STRING        NOT NULL  COMMENT 'Event level: INFO | WARN | ERROR | METRICS',
  event_type                  STRING        NOT NULL  DEFAULT 'stream_progress'
                                                      COMMENT 'Event type, always stream_progress for metrics',
  maturity_level              STRING                  COMMENT 'API maturity: STABLE | EVOLVING',
  details                     STRING        NOT NULL  COMMENT 'JSON object containing stream_progress matching event log schema',
  environment                 STRING        NOT NULL  COMMENT 'dev | stage | prod',
  workspace_id                STRING                  COMMENT 'Databricks workspace identifier',
  domain                      STRING                  COMMENT 'Business domain tag',
  owner                       STRING                  COMMENT 'Stream owner / team',
  criticality                 STRING                  COMMENT 'low | medium | high | critical',
  ingestion_ts                TIMESTAMP     NOT NULL  DEFAULT current_timestamp()
                                                      COMMENT 'Row write time'
)
USING DELTA
CLUSTER BY (event_type, timestamp)
COMMENT 'Streaming metrics in event log format — details column matches stream_progress schema'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false'
);
