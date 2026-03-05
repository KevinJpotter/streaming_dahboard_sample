"""
Streaming Monitoring Listener Package

Usage:
    from listener import MonitoringQueryListener, MetricsWriter

    writer = MetricsWriter(spark, config)
    writer.start()

    listener = MonitoringQueryListener(writer, config)
    spark.streams.addListener(listener)
"""

from listener.streaming_query_listener import MonitoringQueryListener
from listener.metrics_writer import MetricsWriter
from listener.utils import sanitize_query_name, build_tags

__all__ = [
    "MonitoringQueryListener",
    "MetricsWriter",
    "sanitize_query_name",
    "build_tags",
]
