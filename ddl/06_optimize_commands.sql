-- ============================================================
-- Periodic maintenance: OPTIMIZE
-- Liquid clustering handles data layout automatically —
-- no ZORDER needed. OPTIMIZE triggers incremental clustering.
-- Run via scheduled job (e.g., daily off-peak)
-- ============================================================

OPTIMIZE ${catalog}.monitoring.streaming_metrics;

OPTIMIZE ${catalog}.monitoring.stream_errors;

OPTIMIZE ${catalog}.monitoring.data_quality_metrics;

OPTIMIZE ${catalog}.monitoring.sla_latency_metrics;

OPTIMIZE ${catalog}.monitoring.infrastructure_metrics;
