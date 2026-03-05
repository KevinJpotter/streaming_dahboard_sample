"""
Alert configuration for Databricks SQL Alerts.

Defines the 5 core alerts with their escalation rules,
schedules, and notification targets.
"""

ALERTS = [
    {
        "name": "streaming_sla_breach",
        "display_name": "Streaming SLA Breach Detected",
        "query_name": "alert_sla_breach",
        "schedule_interval_seconds": 300,  # every 5 min
        "condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "breach_count"},
            "threshold": 0,
        },
        "severity": "critical",
        "escalation": "pagerduty",
        "rearm_seconds": 600,
        "description": "Fires when any stream exceeds its SLA latency threshold.",
    },
    {
        "name": "streaming_sustained_backpressure",
        "display_name": "Sustained Backpressure (5+ batches)",
        "query_name": "alert_sustained_backpressure",
        "schedule_interval_seconds": 300,
        "condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "backpressured_count"},
            "threshold": 4,
        },
        "severity": "warning",
        "escalation": "slack",
        "rearm_seconds": 900,
        "description": "Fires when input > processed for 5+ consecutive batches.",
    },
    {
        "name": "streaming_batch_duration_spike",
        "display_name": "Batch Duration Spike (>3x avg)",
        "query_name": "alert_batch_duration_spike",
        "schedule_interval_seconds": 300,
        "condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "spike_factor"},
            "threshold": 3.0,
        },
        "severity": "warning",
        "escalation": "slack",
        "rearm_seconds": 600,
        "description": "Fires when batch duration exceeds 3x the rolling average.",
    },
    {
        "name": "streaming_stream_terminated",
        "display_name": "Stream Terminated",
        "query_name": "alert_stream_terminated",
        "schedule_interval_seconds": 60,  # check every minute
        "condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "query_id"},
            "threshold": 0,
            "op_type": "COUNT",
        },
        "severity": "failure",
        "escalation": "pagerduty",
        "rearm_seconds": 300,
        "description": "Fires immediately when any streaming query terminates.",
    },
    {
        "name": "streaming_state_memory_growth",
        "display_name": "State Memory Growth >20% in 30min",
        "query_name": "alert_state_memory_growth",
        "schedule_interval_seconds": 600,  # every 10 min
        "condition": {
            "op": "GREATER_THAN",
            "operand": {"column": "growth_pct"},
            "threshold": 20.0,
        },
        "severity": "warning",
        "escalation": "slack",
        "rearm_seconds": 1800,
        "description": "Fires when state store memory grows >20% within 30 minutes.",
    },
]

# Escalation mapping: severity → notification channel
ESCALATION_MAP = {
    "warning": {
        "channel": "slack",
        "description": "Post to Slack #streaming-alerts channel",
    },
    "critical": {
        "channel": "pagerduty",
        "description": "Page on-call via PagerDuty",
    },
    "failure": {
        "channel": "pagerduty",
        "description": "Immediate PagerDuty escalation",
    },
}
