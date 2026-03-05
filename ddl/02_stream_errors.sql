-- ============================================================
-- stream_errors: failure and termination events
-- ============================================================

CREATE TABLE IF NOT EXISTS ${catalog}.monitoring.stream_errors (
  query_id              STRING        NOT NULL  COMMENT 'Streaming query identifier',
  query_name            STRING                  COMMENT 'Human-readable stream name',
  batch_id              BIGINT                  COMMENT 'Batch that failed (if applicable)',
  timestamp             TIMESTAMP     NOT NULL  COMMENT 'Error occurrence time',
  exception_type        STRING                  COMMENT 'Java/Python exception class name',
  exception_message     STRING                  COMMENT 'Full error message',
  termination_reason    STRING                  COMMENT 'Query termination reason',
  restart_count         INT                     COMMENT 'Number of restarts since first launch',
  stack_trace           STRING                  COMMENT 'Truncated stack trace',
  environment           STRING        NOT NULL  COMMENT 'dev | stage | prod',
  workspace_id          STRING                  COMMENT 'Databricks workspace identifier',
  ingestion_ts          TIMESTAMP     NOT NULL  DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, timestamp)
COMMENT 'Stream error and termination events';
