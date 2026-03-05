-- ============================================================
-- Dataset 6: Data Quality
-- Duplicate rate, dropped records, late record %, schema changes
-- Aggregated hourly
-- ============================================================

SELECT
  query_name,
  window(timestamp, '1 hour').start                  AS time_bucket,
  ROUND(AVG(duplicate_rate) * 100, 2)                AS avg_duplicate_rate_pct,
  SUM(dropped_record_count)                          AS total_dropped_records,
  ROUND(AVG(late_record_percentage) * 100, 2)        AS avg_late_record_pct,
  SUM(CASE WHEN schema_change_detected THEN 1 ELSE 0 END) AS schema_change_count,
  COUNT(*)                                           AS batch_count
FROM ${catalog}.monitoring.data_quality_metrics
WHERE environment = :environment
  AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
GROUP BY query_name, window(timestamp, '1 hour').start
ORDER BY query_name, time_bucket
