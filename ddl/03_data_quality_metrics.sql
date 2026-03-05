-- ============================================================
-- data_quality_metrics: per-batch quality signals
-- ============================================================

CREATE TABLE IF NOT EXISTS ${catalog}.monitoring.data_quality_metrics (
  query_id                STRING              NOT NULL  COMMENT 'Streaming query identifier',
  query_name              STRING                        COMMENT 'Human-readable stream name',
  batch_id                BIGINT              NOT NULL  COMMENT 'Micro-batch sequence number',
  timestamp               TIMESTAMP           NOT NULL  COMMENT 'Batch completion timestamp',
  null_rate_by_column     MAP<STRING, DOUBLE>           COMMENT 'Column name → null fraction',
  duplicate_rate          DOUBLE                        COMMENT 'Fraction of duplicates in batch',
  dropped_record_count    BIGINT                        COMMENT 'Records filtered / dropped',
  schema_change_detected  BOOLEAN                       COMMENT 'True if schema drift detected',
  late_record_percentage  DOUBLE                        COMMENT 'Fraction of records past watermark',
  environment             STRING              NOT NULL  COMMENT 'dev | stage | prod',
  workspace_id            STRING                        COMMENT 'Databricks workspace identifier',
  ingestion_ts            TIMESTAMP           NOT NULL  DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (query_name, timestamp)
COMMENT 'Per-batch data quality metrics from quality callbacks';
