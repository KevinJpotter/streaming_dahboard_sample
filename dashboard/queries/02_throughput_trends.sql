-- ============================================================
-- Dataset 2: Throughput Trends
-- Input vs processed rows/sec, batch duration, watermark delay
-- 1-minute time buckets — extracts from details JSON
-- ============================================================

SELECT
  details:stream_progress.name::STRING                                    AS query_name,
  window(timestamp, '1 minute').start                                     AS time_bucket,
  ROUND(AVG(details:stream_progress.inputRowsPerSecond::DOUBLE), 2)       AS avg_input_rows_per_sec,
  ROUND(AVG(details:stream_progress.processedRowsPerSecond::DOUBLE), 2)   AS avg_processed_rows_per_sec,
  ROUND(AVG(details:stream_progress.batchDuration::BIGINT), 0)            AS avg_batch_duration_ms,
  MAX(details:stream_progress.batchDuration::BIGINT)                      AS max_batch_duration_ms,
  SUM(details:stream_progress.numInputRows::BIGINT)                       AS total_input_rows,
  ROUND(
    AVG(
      unix_timestamp(timestamp) -
      unix_timestamp(details:stream_progress.eventTime.watermark::TIMESTAMP)
    ), 0
  )                                                                       AS avg_watermark_delay_sec
FROM ${catalog}.monitoring.streaming_metrics
WHERE environment = :environment
  AND CAST(timestamp AS DATE) >= current_date() - INTERVAL :time_range_days DAY
GROUP BY details:stream_progress.name::STRING, window(timestamp, '1 minute').start
ORDER BY query_name, time_bucket
