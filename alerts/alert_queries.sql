-- ============================================================
-- Alert 1: SLA Breach Detected
-- Fires when any stream has an SLA breach in the last 10 minutes
-- ============================================================

-- alert_sla_breach
SELECT
  query_name,
  COUNT(*) AS breach_count,
  MAX(latency_ms) AS max_latency_ms,
  MAX(sla_threshold_ms) AS sla_threshold_ms
FROM ${catalog}.monitoring.sla_latency_metrics
WHERE environment = '${environment}'
  AND sla_breach_flag = true
  AND timestamp >= current_timestamp() - INTERVAL 10 MINUTE
GROUP BY query_name
HAVING COUNT(*) > 0;


-- ============================================================
-- Alert 2: Sustained Backpressure (5+ consecutive batches)
-- Extracts from details JSON
-- ============================================================

-- alert_sustained_backpressure
WITH recent_batches AS (
  SELECT
    details:stream_progress.id::STRING                        AS query_id,
    details:stream_progress.name::STRING                      AS query_name,
    details:stream_progress.batchId::BIGINT                   AS batch_id,
    CASE
      WHEN details:stream_progress.inputRowsPerSecond::DOUBLE >
           details:stream_progress.processedRowsPerSecond::DOUBLE THEN 1
      ELSE 0
    END AS is_backpressured,
    ROW_NUMBER() OVER (
      PARTITION BY details:stream_progress.id::STRING
      ORDER BY details:stream_progress.batchId::BIGINT DESC
    ) AS rn
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = '${environment}'
    AND timestamp >= current_timestamp() - INTERVAL 30 MINUTE
),
last_5 AS (
  SELECT
    query_id,
    query_name,
    SUM(is_backpressured) AS backpressured_count
  FROM recent_batches
  WHERE rn <= 5
  GROUP BY query_id, query_name
)
SELECT
  query_name,
  backpressured_count
FROM last_5
WHERE backpressured_count >= 5;


-- ============================================================
-- Alert 3: Batch Duration Spike (>3x rolling average)
-- Extracts from details JSON
-- ============================================================

-- alert_batch_duration_spike
WITH rolling AS (
  SELECT
    details:stream_progress.name::STRING                      AS query_name,
    details:stream_progress.id::STRING                        AS query_id,
    details:stream_progress.batchId::BIGINT                   AS batch_id,
    details:stream_progress.batchDuration::BIGINT             AS batch_duration_ms,
    AVG(details:stream_progress.batchDuration::BIGINT) OVER (
      PARTITION BY details:stream_progress.id::STRING
      ORDER BY details:stream_progress.batchId::BIGINT
      ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    ) AS rolling_avg_duration_ms
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = '${environment}'
    AND timestamp >= current_timestamp() - INTERVAL 1 HOUR
)
SELECT
  query_name,
  batch_id,
  batch_duration_ms,
  ROUND(rolling_avg_duration_ms, 0) AS rolling_avg_duration_ms,
  ROUND(batch_duration_ms / NULLIF(rolling_avg_duration_ms, 0), 2) AS spike_factor
FROM rolling
WHERE batch_duration_ms > 3.0 * rolling_avg_duration_ms
  AND rolling_avg_duration_ms IS NOT NULL
ORDER BY spike_factor DESC
LIMIT 10;


-- ============================================================
-- Alert 4: Stream Terminated
-- Fires on any termination event in the last 10 minutes
-- ============================================================

-- alert_stream_terminated
SELECT
  query_id,
  query_name,
  timestamp AS terminated_at,
  exception_type,
  exception_message,
  termination_reason,
  restart_count
FROM ${catalog}.monitoring.stream_errors
WHERE environment = '${environment}'
  AND timestamp >= current_timestamp() - INTERVAL 10 MINUTE
ORDER BY timestamp DESC;


-- ============================================================
-- Alert 5: State Memory Growth (>20% in 30 minutes)
-- Extracts stateOperators from details JSON
-- ============================================================

-- alert_state_memory_growth
WITH memory_window AS (
  SELECT
    details:stream_progress.id::STRING                        AS query_id,
    details:stream_progress.name::STRING                      AS query_name,
    AGGREGATE(
      from_json(details:stream_progress.stateOperators, 'ARRAY<STRUCT<memoryUsedBytes:BIGINT>>'),
      CAST(0 AS BIGINT),
      (acc, x) -> acc + COALESCE(x.memoryUsedBytes, 0)
    )                                                         AS state_memory_bytes,
    timestamp,
    FIRST_VALUE(
      AGGREGATE(
        from_json(details:stream_progress.stateOperators, 'ARRAY<STRUCT<memoryUsedBytes:BIGINT>>'),
        CAST(0 AS BIGINT),
        (acc, x) -> acc + COALESCE(x.memoryUsedBytes, 0)
      )
    ) OVER (
      PARTITION BY details:stream_progress.id::STRING
      ORDER BY timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS memory_30min_ago,
    LAST_VALUE(
      AGGREGATE(
        from_json(details:stream_progress.stateOperators, 'ARRAY<STRUCT<memoryUsedBytes:BIGINT>>'),
        CAST(0 AS BIGINT),
        (acc, x) -> acc + COALESCE(x.memoryUsedBytes, 0)
      )
    ) OVER (
      PARTITION BY details:stream_progress.id::STRING
      ORDER BY timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS memory_now
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = '${environment}'
    AND timestamp >= current_timestamp() - INTERVAL 30 MINUTE
)
SELECT DISTINCT
  query_name,
  ROUND(memory_30min_ago / (1024.0 * 1024.0), 2) AS memory_start_mb,
  ROUND(memory_now / (1024.0 * 1024.0), 2) AS memory_end_mb,
  ROUND(100.0 * (memory_now - memory_30min_ago) / NULLIF(memory_30min_ago, 0), 2) AS growth_pct
FROM memory_window
WHERE memory_30min_ago > 0
  AND (100.0 * (memory_now - memory_30min_ago) / memory_30min_ago) > 20.0;
