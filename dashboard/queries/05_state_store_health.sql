-- ============================================================
-- Dataset 5: State Store Health
-- Memory usage trend, state row growth, memory growth rate
-- Extracts stateOperators from details JSON
-- ============================================================

WITH state_data AS (
  SELECT
    details:stream_progress.name::STRING                          AS query_name,
    details:stream_progress.id::STRING                            AS query_id,
    timestamp,
    details:stream_progress.batchId::BIGINT                       AS batch_id,
    AGGREGATE(
      from_json(details:stream_progress.stateOperators, 'ARRAY<STRUCT<memoryUsedBytes:BIGINT, numRowsTotal:BIGINT>>'),
      CAST(0 AS BIGINT),
      (acc, x) -> acc + COALESCE(x.memoryUsedBytes, 0)
    )                                                             AS state_operator_memory_bytes,
    AGGREGATE(
      from_json(details:stream_progress.stateOperators, 'ARRAY<STRUCT<memoryUsedBytes:BIGINT, numRowsTotal:BIGINT>>'),
      CAST(0 AS BIGINT),
      (acc, x) -> acc + COALESCE(x.numRowsTotal, 0)
    )                                                             AS state_rows_total
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = :environment
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
),
with_lag AS (
  SELECT
    query_name,
    timestamp,
    batch_id,
    state_operator_memory_bytes,
    state_rows_total,
    ROUND(state_operator_memory_bytes / (1024.0 * 1024.0), 2)    AS state_memory_mb,
    LAG(state_operator_memory_bytes) OVER (
      PARTITION BY query_id ORDER BY batch_id
    )                                                             AS prev_memory_bytes
  FROM state_data
  WHERE state_operator_memory_bytes > 0
)
SELECT
  query_name,
  timestamp,
  batch_id,
  state_memory_mb,
  state_rows_total,
  CASE
    WHEN prev_memory_bytes > 0
    THEN ROUND(
      100.0 * (state_operator_memory_bytes - prev_memory_bytes) / prev_memory_bytes,
      2
    )
    ELSE 0
  END AS memory_growth_pct
FROM with_lag
ORDER BY query_name, batch_id
