-- ============================================================
-- Dataset 7: Infrastructure & Cost
-- CPU/memory %, executor count, DBU consumption, autoscale events
-- Aggregated hourly
-- ============================================================

SELECT
  cluster_id,
  window(timestamp, '1 hour').start                  AS time_bucket,
  ROUND(AVG(cpu_usage_percent), 1)                   AS avg_cpu_pct,
  MAX(cpu_usage_percent)                             AS max_cpu_pct,
  ROUND(AVG(memory_usage_percent), 1)                AS avg_memory_pct,
  MAX(memory_usage_percent)                          AS max_memory_pct,
  ROUND(AVG(executor_count), 0)                      AS avg_executor_count,
  MAX(executor_count)                                AS max_executor_count,
  ROUND(SUM(COALESCE(dbu_consumption, 0)), 2)        AS total_dbu,
  SUM(CASE WHEN autoscaling_event = 'SCALE_UP' THEN 1 ELSE 0 END)   AS scale_up_count,
  SUM(CASE WHEN autoscaling_event = 'SCALE_DOWN' THEN 1 ELSE 0 END) AS scale_down_count
FROM ${catalog}.monitoring.infrastructure_metrics
WHERE environment = :environment
  AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
GROUP BY cluster_id, window(timestamp, '1 hour').start
ORDER BY cluster_id, time_bucket
