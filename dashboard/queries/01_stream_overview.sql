-- ============================================================
-- Dataset 1: Stream Overview
-- Current status, uptime, last batch, SLA breach indicator
-- Reads from event-log-format streaming_metrics table
-- ============================================================

WITH latest_batch AS (
  SELECT
    details:stream_progress.id::STRING                AS query_id,
    details:stream_progress.name::STRING              AS query_name,
    MAX(timestamp)                                    AS last_batch_ts,
    MAX(details:stream_progress.batchId::BIGINT)      AS last_batch_id,
    MIN(timestamp)                                    AS first_batch_ts
  FROM ${catalog}.monitoring.streaming_metrics
  WHERE environment = :environment
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
  GROUP BY
    details:stream_progress.id::STRING,
    details:stream_progress.name::STRING
),
error_status AS (
  SELECT
    query_id,
    MAX(timestamp) AS last_error_ts
  FROM ${catalog}.monitoring.stream_errors
  WHERE environment = :environment
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
  GROUP BY query_id
),
sla_breaches AS (
  SELECT
    query_id,
    COUNT(*) AS breach_count
  FROM ${catalog}.monitoring.sla_latency_metrics
  WHERE environment = :environment
    AND sla_breach_flag = true
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
  GROUP BY query_id
)
SELECT
  lb.query_id,
  lb.query_name,
  CASE
    WHEN es.last_error_ts > lb.last_batch_ts THEN 'ERROR'
    WHEN lb.last_batch_ts < current_timestamp() - INTERVAL 10 MINUTE THEN 'INACTIVE'
    ELSE 'ACTIVE'
  END                                               AS stream_status,
  lb.last_batch_ts,
  lb.last_batch_id,
  lb.first_batch_ts,
  CAST(
    (unix_timestamp(lb.last_batch_ts) - unix_timestamp(lb.first_batch_ts)) / 3600.0
    AS DECIMAL(10,1)
  )                                                 AS uptime_hours,
  COALESCE(sb.breach_count, 0)                      AS sla_breach_count,
  CASE
    WHEN COALESCE(sb.breach_count, 0) > 0 THEN true
    ELSE false
  END                                               AS has_sla_breaches,
  es.last_error_ts
FROM latest_batch lb
LEFT JOIN error_status es ON lb.query_id = es.query_id
LEFT JOIN sla_breaches sb ON lb.query_id = sb.query_id
ORDER BY lb.query_name
