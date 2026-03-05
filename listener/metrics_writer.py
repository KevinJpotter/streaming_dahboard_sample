"""
Async queue-based Delta writer for monitoring metrics.

A daemon thread drains a queue and batch-writes records to Delta tables.
Flushes when batch reaches WRITER_BATCH_SIZE or WRITER_FLUSH_INTERVAL_SECONDS.
"""

import logging
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType, MapType,
)

logger = logging.getLogger("streaming_monitor.writer")

# Explicit schemas for each monitoring table — avoids type inference failures
# on Spark Connect when nullable fields are all None in a small batch.
_SCHEMAS = {
    "streaming_metrics": StructType([
        StructField("id", StringType(), False),
        StructField("sequence", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("message", StringType(), True),
        StructField("level", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("maturity_level", StringType(), True),
        StructField("details", StringType(), False),
        StructField("environment", StringType(), False),
        StructField("workspace_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("criticality", StringType(), True),
    ]),
    "stream_errors": StructType([
        StructField("query_id", StringType(), False),
        StructField("query_name", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("exception_type", StringType(), True),
        StructField("exception_message", StringType(), True),
        StructField("termination_reason", StringType(), True),
        StructField("restart_count", IntegerType(), True),
        StructField("stack_trace", StringType(), True),
        StructField("environment", StringType(), False),
        StructField("workspace_id", StringType(), True),
    ]),
    "sla_latency_metrics": StructType([
        StructField("query_id", StringType(), False),
        StructField("query_name", StringType(), True),
        StructField("batch_id", LongType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("event_time_min", TimestampType(), True),
        StructField("event_time_max", TimestampType(), True),
        StructField("processing_time", TimestampType(), True),
        StructField("latency_ms", LongType(), True),
        StructField("latency_p50", LongType(), True),
        StructField("latency_p90", LongType(), True),
        StructField("latency_p99", LongType(), True),
        StructField("max_latency", LongType(), True),
        StructField("sla_breach_flag", BooleanType(), True),
        StructField("sla_threshold_ms", LongType(), True),
        StructField("environment", StringType(), False),
        StructField("workspace_id", StringType(), True),
    ]),
    "data_quality_metrics": StructType([
        StructField("query_id", StringType(), False),
        StructField("query_name", StringType(), True),
        StructField("batch_id", LongType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("null_rate_by_column", StringType(), True),
        StructField("duplicate_rate", DoubleType(), True),
        StructField("dropped_record_count", IntegerType(), True),
        StructField("schema_change_detected", BooleanType(), True),
        StructField("late_record_percentage", DoubleType(), True),
        StructField("environment", StringType(), False),
        StructField("workspace_id", StringType(), True),
    ]),
    "infrastructure_metrics": StructType([
        StructField("cluster_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("cpu_usage_percent", DoubleType(), True),
        StructField("memory_usage_percent", DoubleType(), True),
        StructField("executor_count", IntegerType(), True),
        StructField("autoscaling_event", StringType(), True),
        StructField("dbu_consumption", DoubleType(), True),
        StructField("driver_node_type", StringType(), True),
        StructField("worker_node_type", StringType(), True),
        StructField("environment", StringType(), False),
        StructField("workspace_id", StringType(), True),
    ]),
}


class MetricsWriter:
    """
    Daemon thread that drains a queue and writes batches to Delta tables.

    Usage:
        writer = MetricsWriter(spark, config)
        writer.start()
        writer.enqueue({"table": "streaming_metrics", "query_id": "...", ...})
    """

    def __init__(self, spark, config: Dict[str, Any]):
        self._spark = spark
        self._catalog = config.get("catalog", "main")
        self._schema = config.get("schema", "monitoring")
        self._environment = config.get("environment", "prod")
        self._batch_size = config.get("writer_batch_size", 50)
        self._flush_interval = config.get("writer_flush_interval_seconds", 10)
        self._max_queue_size = config.get("writer_queue_max_size", 10_000)

        self._queue: queue.Queue = queue.Queue(maxsize=self._max_queue_size)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False

        # Table name → full path mapping
        self._table_paths = {
            "streaming_metrics": f"{self._catalog}.{self._schema}.streaming_metrics",
            "stream_errors": f"{self._catalog}.{self._schema}.stream_errors",
            "data_quality_metrics": f"{self._catalog}.{self._schema}.data_quality_metrics",
            "sla_latency_metrics": f"{self._catalog}.{self._schema}.sla_latency_metrics",
            "infrastructure_metrics": f"{self._catalog}.{self._schema}.infrastructure_metrics",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background writer daemon thread."""
        if self._started:
            logger.warning("MetricsWriter already started")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="metrics-writer-daemon",
            daemon=True,
        )
        self._thread.start()
        self._started = True
        logger.info("MetricsWriter started (batch_size=%d, flush_interval=%ds)",
                     self._batch_size, self._flush_interval)

    def stop(self, timeout: float = 30.0) -> None:
        """Signal the writer to flush remaining records and stop."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._started = False
        logger.info("MetricsWriter stopped")

    def enqueue(self, record: Dict[str, Any]) -> None:
        """Push a metric record onto the queue (non-blocking, drops on overflow)."""
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            logger.warning("Metrics queue full — dropping record for table=%s",
                           record.get("table", "unknown"))

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    # ------------------------------------------------------------------
    # Writer loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Main loop: drain queue, group by table, flush batches."""
        buffers: Dict[str, List[Dict]] = defaultdict(list)
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            try:
                record = self._queue.get(timeout=1.0)
                table = record.pop("table", None)
                if table and table in self._table_paths:
                    buffers[table].append(record)
            except queue.Empty:
                pass

            now = time.monotonic()
            elapsed = now - last_flush

            for table_name in list(buffers.keys()):
                buf = buffers[table_name]
                if len(buf) >= self._batch_size or (elapsed >= self._flush_interval and buf):
                    self._flush(table_name, buf)
                    buffers[table_name] = []

            if elapsed >= self._flush_interval:
                last_flush = now

        # Final flush on shutdown
        for table_name, buf in buffers.items():
            if buf:
                self._flush(table_name, buf)
        logger.info("Writer loop exited — all buffers flushed")

    def _flush(self, table_name: str, records: List[Dict]) -> None:
        """Write a batch of records to a Delta table."""
        full_table = self._table_paths[table_name]
        try:
            schema = _SCHEMAS.get(table_name)
            if schema:
                # Convert dict values that need serialization (e.g. dict → JSON string)
                for rec in records:
                    for field in schema.fields:
                        if field.name in rec and isinstance(rec[field.name], dict):
                            import json
                            rec[field.name] = json.dumps(rec[field.name])
                df = self._spark.createDataFrame(records, schema=schema)
            else:
                df = self._spark.createDataFrame(records)
            df.write.format("delta").mode("append").saveAsTable(full_table)
            logger.debug("Flushed %d records to %s", len(records), full_table)
        except Exception:
            logger.exception("Failed to flush %d records to %s", len(records), full_table)


class InfrastructurePoller:
    """
    Polls the Databricks Clusters REST API for CPU, memory, executor metrics
    and pushes them onto the MetricsWriter queue.

    Usage:
        poller = InfrastructurePoller(writer, config)
        poller.start()
    """

    def __init__(self, metrics_writer: MetricsWriter, config: Dict[str, Any]):
        self._writer = metrics_writer
        self._cluster_id = config.get("cluster_id", "")
        self._environment = config.get("environment", "prod")
        self._workspace_id = config.get("workspace_id", "")
        self._poll_interval = config.get("infra_poll_interval_seconds", 60)
        self._host = config.get("databricks_host", "")
        self._token = config.get("databricks_token", "")
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="infra-poller-daemon",
            daemon=True,
        )
        self._thread.start()
        logger.info("InfrastructurePoller started (interval=%ds)", self._poll_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)

    def _run(self) -> None:
        import requests

        while not self._stop_event.is_set():
            try:
                self._poll(requests)
            except Exception:
                logger.exception("Infrastructure poll failed")
            self._stop_event.wait(self._poll_interval)

    def _poll(self, requests) -> None:
        if not self._host or not self._token or not self._cluster_id:
            return

        url = f"{self._host}/api/2.0/clusters/get"
        headers = {"Authorization": f"Bearer {self._token}"}
        resp = requests.get(url, headers=headers, json={"cluster_id": self._cluster_id}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        executor_count = data.get("num_workers", 0)
        autoscale = data.get("autoscale", {})
        prev_workers = getattr(self, "_prev_workers", None)
        autoscaling_event = None
        if prev_workers is not None:
            if executor_count > prev_workers:
                autoscaling_event = "SCALE_UP"
            elif executor_count < prev_workers:
                autoscaling_event = "SCALE_DOWN"
        self._prev_workers = executor_count

        record = {
            "table": "infrastructure_metrics",
            "cluster_id": self._cluster_id,
            "timestamp": datetime.utcnow(),
            "cpu_usage_percent": None,      # Not available from clusters/get
            "memory_usage_percent": None,    # Requires Ganglia/metrics API
            "executor_count": executor_count,
            "autoscaling_event": autoscaling_event,
            "dbu_consumption": None,         # Requires billing API
            "driver_node_type": data.get("driver_node_type_id"),
            "worker_node_type": data.get("node_type_id"),
            "environment": self._environment,
            "workspace_id": self._workspace_id,
        }
        self._writer.enqueue(record)
