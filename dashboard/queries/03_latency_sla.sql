-- ============================================================
-- Dataset 3: Latency & SLA
-- P50/P90/P99 latency trends, breach count and percentage
-- ============================================================

WITH latency_stats AS (
  SELECT
    query_name,
    window(timestamp, '5 minutes').start             AS time_bucket,
    PERCENTILE_APPROX(latency_ms, 0.50)              AS p50_latency_ms,
    PERCENTILE_APPROX(latency_ms, 0.90)              AS p90_latency_ms,
    PERCENTILE_APPROX(latency_ms, 0.99)              AS p99_latency_ms,
    MAX(max_latency)                                  AS max_latency_ms,
    COUNT(*)                                          AS batch_count,
    SUM(CASE WHEN sla_breach_flag THEN 1 ELSE 0 END) AS breach_count,
    sla_threshold_ms
  FROM ${catalog}.monitoring.sla_latency_metrics
  WHERE environment = :environment
    AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
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
