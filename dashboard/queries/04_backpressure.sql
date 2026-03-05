-- ============================================================
-- Dataset 4: Backpressure Detection
-- Input/processed ratio, 5-batch sustained imbalance window
-- Extracts from details JSON
-- ============================================================

WITH batch_ratios AS (
  SELECT
    details:stream_progress.id::STRING                            AS query_id,
    details:stream_progress.name::STRING                          AS query_name,
    details:stream_progress.batchId::BIGINT                       AS batch_id,
    timestamp,
    details:stream_progress.inputRowsPerSecond::DOUBLE            AS input_rows_per_second,
    details:stream_progress.processedRowsPerSecond::DOUBLE        AS processed_rows_per_second,
    CASE
      WHEN details:stream_progress.processedRowsPerSecond::DOUBLE > 0
      THEN ROUND(
        details:stream_progress.inputRowsPerSecond::DOUBLE /
        details:stream_progress.processedRowsPerSecond::DOUBLE,
        3
      )
      ELSE NULL
    END                                                           AS backpressure_ratio,
    CASE
      WHEN details:stream_progress.inputRowsPerSecond::DOUBLE >
           details:stream_progress.processedRowsPerSecond::DOUBLE THEN 1
      ELSE 0
    END                                                           AS is_backpressured
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = :environment
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
),
windowed AS (
  SELECT
    *,
    SUM(is_backpressured) OVER (
      PARTITION BY query_id
      ORDER BY batch_id
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )                                                             AS sustained_count
  FROM batch_ratios
)
SELECT
  query_name,
  batch_id,
  timestamp,
  input_rows_per_second,
  processed_rows_per_second,
  backpressure_ratio,
  sustained_count,
  CASE
    WHEN sustained_count >= 5 THEN 'CRITICAL'
    WHEN sustained_count >= 3 THEN 'WARNING'
    ELSE 'OK'
  END                                                             AS backpressure_status
FROM windowed
ORDER BY query_name, batch_id DESC
