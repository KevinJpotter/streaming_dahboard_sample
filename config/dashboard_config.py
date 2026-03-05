"""
Streaming Monitoring Dashboard Configuration

Central configuration for catalog, schema, environment, SLA thresholds,
and alerting parameters. All monitoring components reference this config.
"""

# ---------------------------------------------------------------------------
# Unity Catalog location
# ---------------------------------------------------------------------------
CATALOG = "main"
SCHEMA = "monitoring"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ENVIRONMENT = "prod"  # dev | stage | prod
WORKSPACE_ID = None   # populated at runtime; None on serverless

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
TABLES = {
    "streaming_metrics": f"{FULL_SCHEMA}.streaming_metrics",
    "stream_errors": f"{FULL_SCHEMA}.stream_errors",
    "data_quality_metrics": f"{FULL_SCHEMA}.data_quality_metrics",
    "sla_latency_metrics": f"{FULL_SCHEMA}.sla_latency_metrics",
    "infrastructure_metrics": f"{FULL_SCHEMA}.infrastructure_metrics",
}

# ---------------------------------------------------------------------------
# SLA thresholds
# ---------------------------------------------------------------------------
SLA_LATENCY_WARNING_MS = 30_000       # 30 s
SLA_LATENCY_CRITICAL_MS = 60_000      # 60 s
SLA_BATCH_DURATION_WARNING_MS = 30_000
SLA_BATCH_DURATION_CRITICAL_MS = 60_000

# ---------------------------------------------------------------------------
# Backpressure detection
# ---------------------------------------------------------------------------
BACKPRESSURE_RATIO_THRESHOLD = 1.0    # input/processed > 1.0
BACKPRESSURE_SUSTAINED_BATCHES = 5    # consecutive batches

# ---------------------------------------------------------------------------
# State store alerts
# ---------------------------------------------------------------------------
STATE_MEMORY_GROWTH_PERCENT = 20      # >20 % growth in 30 min triggers alert

# ---------------------------------------------------------------------------
# Batch duration spike
# ---------------------------------------------------------------------------
BATCH_DURATION_SPIKE_FACTOR = 3.0     # >3x rolling average

# ---------------------------------------------------------------------------
# Metrics writer tuning
# ---------------------------------------------------------------------------
WRITER_BATCH_SIZE = 50                # flush after N records
WRITER_FLUSH_INTERVAL_SECONDS = 10    # or flush after N seconds
WRITER_QUEUE_MAX_SIZE = 10_000        # back-pressure safety valve

# ---------------------------------------------------------------------------
# Infrastructure poller
# ---------------------------------------------------------------------------
INFRA_POLL_INTERVAL_SECONDS = 60

# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------
ALERTING = {
    "slack_webhook_url": "",           # populate before deploy
    "pagerduty_routing_key": "",       # populate before deploy
    "email_recipients": [],
    "escalation": {
        "warning": "slack",
        "critical": "pagerduty",
        "failure": "pagerduty",        # immediate escalation
    },
}

# ---------------------------------------------------------------------------
# Dashboard parameters (used in Lakeview definition)
# ---------------------------------------------------------------------------
DASHBOARD_TIME_RANGE_DAYS = 7
