"""
StreamingQueryListener implementation for monitoring.

Logs metrics in the same format as the Databricks event log table.
The `details` field contains a `stream_progress` object that mirrors
the event log's stream_progress event schema.

Reference:
  https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema

All I/O is deferred to MetricsWriter — callbacks return in microseconds.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql.streaming import StreamingQueryListener

from listener.utils import (
    compute_latency_ms,
    safe_get,
    sanitize_query_name,
)

logger = logging.getLogger("streaming_monitor.listener")


class MonitoringQueryListener(StreamingQueryListener):
    """
    Non-blocking listener that captures the full StreamingQueryProgress
    JSON and writes it in event-log-compatible format with a `details`
    column containing `stream_progress`.

    Usage:
        writer = MetricsWriter(spark, config)
        writer.start()
        listener = MonitoringQueryListener(writer, config)
        spark.streams.addListener(listener)
    """

    def __init__(
        self,
        metrics_writer,
        config: Dict[str, Any],
        quality_callbacks: Optional[List[Callable]] = None,
    ):
        super().__init__()
        self._writer = metrics_writer
        self._config = config
        self._environment = config.get("environment", "prod")
        self._workspace_id = config.get("workspace_id", "")
        self._job_id = config.get("job_id", "")
        self._sla_threshold_ms = config.get("sla_latency_critical_ms", 60_000)
        self._quality_callbacks: List[Callable] = quality_callbacks or []
        self._tags: Dict[str, Dict[str, str]] = {}  # query_id → tags
        self._restart_counts: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_quality_callback(self, callback: Callable) -> None:
        """Register a function called per batch that returns quality metrics.

        Signature: callback(query_id, batch_id, progress_json) → dict | None
        Returned dict is written to data_quality_metrics.
        """
        self._quality_callbacks.append(callback)

    def set_tags(self, query_id: str, domain: str = "", owner: str = "",
                 criticality: str = "medium") -> None:
        """Set business tags for a specific query."""
        self._tags[query_id] = {
            "domain": domain,
            "owner": owner,
            "criticality": criticality,
        }

    # ------------------------------------------------------------------
    # Listener callbacks — must return fast
    # ------------------------------------------------------------------

    def onQueryStarted(self, event) -> None:
        query_id = str(event.id)
        query_name = event.name or "unnamed_stream"
        logger.info("Stream started: %s (%s)", query_name, query_id)

    def onQueryProgress(self, event) -> None:
        try:
            # Spark Connect: .json is a str property; classic Spark: .json() is a method
            raw = event.progress.json
            if callable(raw):
                raw = raw()
            progress = json.loads(raw)
            self._handle_progress(progress)
        except Exception:
            logger.exception("Error processing query progress event")

    def onQueryTerminated(self, event) -> None:
        query_id = str(event.id)
        self._restart_counts[query_id] = self._restart_counts.get(query_id, 0) + 1
        error_record = {
            "table": "stream_errors",
            "query_id": query_id,
            "query_name": None,
            "batch_id": None,
            "timestamp": datetime.utcnow(),
            "exception_type": getattr(event, "exception", None) and type(event.exception).__name__,
            "exception_message": str(getattr(event, "exception", None)),
            "termination_reason": getattr(event, "errorClassOnException", "UNKNOWN"),
            "restart_count": self._restart_counts.get(query_id, 0),
            "stack_trace": None,
            "environment": self._environment,
            "workspace_id": self._workspace_id,
        }
        self._writer.enqueue(error_record)
        logger.warning("Stream terminated: %s", query_id)

    # ------------------------------------------------------------------
    # Internal — build event-log-compatible records
    # ------------------------------------------------------------------

    def _build_origin(self, query_id: str, query_name: str) -> dict:
        """Build origin metadata matching event log origin field."""
        return {
            "workspace_id": self._workspace_id,
            "job_id": self._job_id,
            "flow_name": query_name,
            "flow_id": query_id,
        }

    def _build_details(self, progress: Dict[str, Any]) -> str:
        """Build the details JSON matching the event log stream_progress format.

        Structure:
          {
            "stream_progress": {
              "id": "...",
              "runId": "...",
              "name": "...",
              "timestamp": "...",
              "batchId": N,
              "batchDuration": N,
              "numInputRows": N,
              "inputRowsPerSecond": N.N,
              "processedRowsPerSecond": N.N,
              "durationMs": { "addBatch": ..., "getBatch": ..., ... },
              "eventTime": { "avg": ..., "max": ..., "min": ..., "watermark": ... },
              "stateOperators": [ { ... } ],
              "sources": [ { ... } ],
              "sink": { ... },
              "observedMetrics": { ... }
            }
          }
        """
        stream_progress = {
            "id": progress.get("id"),
            "runId": progress.get("runId"),
            "name": progress.get("name"),
            "timestamp": progress.get("timestamp"),
            "batchId": progress.get("batchId"),
            "batchDuration": progress.get("batchDuration"),
            "numInputRows": progress.get("numInputRows", 0),
            "inputRowsPerSecond": progress.get("inputRowsPerSecond", 0.0),
            "processedRowsPerSecond": progress.get("processedRowsPerSecond", 0.0),
            "durationMs": progress.get("durationMs", {}),
            "eventTime": progress.get("eventTime", {}),
            "stateOperators": progress.get("stateOperators", []),
            "sources": progress.get("sources", []),
            "sink": progress.get("sink", {}),
            "observedMetrics": progress.get("observedMetrics", {}),
        }
        return json.dumps({"stream_progress": stream_progress})

    def _handle_progress(self, progress: Dict[str, Any]) -> None:
        query_id = progress.get("id", "")
        query_name = sanitize_query_name(progress.get("name"))
        batch_id = progress.get("batchId", -1)
        ts_str = progress.get("timestamp")
        timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else datetime.utcnow()

        tags = self._tags.get(query_id, {})

        # --- Build event-log-compatible streaming_metrics record ---
        details_json = self._build_details(progress)
        origin_json = json.dumps(self._build_origin(query_id, query_name))

        streaming_record = {
            "table": "streaming_metrics",
            "id": str(uuid.uuid4()),
            "sequence": json.dumps({"batch_id": batch_id}),
            "origin": origin_json,
            "timestamp": timestamp,
            "message": f"Completed streaming update for '{query_name}' batch {batch_id}.",
            "level": "METRICS",
            "event_type": "stream_progress",
            "maturity_level": "STABLE",
            "details": details_json,
            "environment": self._environment,
            "workspace_id": self._workspace_id,
            "domain": tags.get("domain", ""),
            "owner": tags.get("owner", ""),
            "criticality": tags.get("criticality", "medium"),
        }
        self._writer.enqueue(streaming_record)

        # --- SLA / latency metrics ---
        event_time = safe_get(progress, "eventTime", default={})
        event_time_max_str = event_time.get("max")
        event_time_min_str = event_time.get("min")
        event_time_max = None
        event_time_min = None
        if event_time_max_str:
            try:
                event_time_max = datetime.fromisoformat(event_time_max_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
        if event_time_min_str:
            try:
                event_time_min = datetime.fromisoformat(event_time_min_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        latency_ms = compute_latency_ms(timestamp, event_time_max)
        sla_breach = latency_ms is not None and latency_ms > self._sla_threshold_ms

        sla_record = {
            "table": "sla_latency_metrics",
            "query_id": query_id,
            "query_name": query_name,
            "batch_id": batch_id,
            "timestamp": timestamp,
            "event_time_min": event_time_min,
            "event_time_max": event_time_max,
            "processing_time": timestamp,
            "latency_ms": latency_ms,
            "latency_p50": latency_ms,  # per-batch: single value = all percentiles
            "latency_p90": latency_ms,
            "latency_p99": latency_ms,
            "max_latency": latency_ms,
            "sla_breach_flag": sla_breach,
            "sla_threshold_ms": self._sla_threshold_ms,
            "environment": self._environment,
            "workspace_id": self._workspace_id,
        }
        self._writer.enqueue(sla_record)

        # --- Data quality callbacks ---
        for cb in self._quality_callbacks:
            try:
                quality_result = cb(query_id, batch_id, progress)
                if quality_result and isinstance(quality_result, dict):
                    quality_record = {
                        "table": "data_quality_metrics",
                        "query_id": query_id,
                        "query_name": query_name,
                        "batch_id": batch_id,
                        "timestamp": timestamp,
                        "null_rate_by_column": quality_result.get("null_rate_by_column"),
                        "duplicate_rate": quality_result.get("duplicate_rate"),
                        "dropped_record_count": quality_result.get("dropped_record_count"),
                        "schema_change_detected": quality_result.get("schema_change_detected", False),
                        "late_record_percentage": quality_result.get("late_record_percentage"),
                        "environment": self._environment,
                        "workspace_id": self._workspace_id,
                    }
                    self._writer.enqueue(quality_record)
            except Exception:
                logger.exception("Quality callback failed for batch %d", batch_id)
