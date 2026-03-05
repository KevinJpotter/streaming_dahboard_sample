-- ============================================================
-- infrastructure_metrics: cluster resource and cost tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS ${catalog}.monitoring.infrastructure_metrics (
  cluster_id          STRING      NOT NULL  COMMENT 'Databricks cluster identifier',
  timestamp           TIMESTAMP   NOT NULL  COMMENT 'Metric collection time',
  cpu_usage_percent   DOUBLE                COMMENT 'Cluster CPU utilization percentage',
  memory_usage_percent DOUBLE               COMMENT 'Cluster memory utilization percentage',
  executor_count      INT                   COMMENT 'Current number of executors',
  autoscaling_event   STRING                COMMENT 'SCALE_UP | SCALE_DOWN | null',
  dbu_consumption     DOUBLE                COMMENT 'DBUs consumed since last poll',
  driver_node_type    STRING                COMMENT 'Driver instance type',
  worker_node_type    STRING                COMMENT 'Worker instance type',
  environment         STRING      NOT NULL  COMMENT 'dev | stage | prod',
  workspace_id        STRING                COMMENT 'Databricks workspace identifier',
  ingestion_ts        TIMESTAMP   NOT NULL  DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (cluster_id, timestamp)
COMMENT 'Cluster infrastructure metrics from REST API polling';
