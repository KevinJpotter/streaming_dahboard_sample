# Claude Context: Streaming Monitoring Dashboard for Databricks

## Purpose
This document provides persistent architectural context and implementation standards for generating, modifying, or reviewing a production-grade streaming monitoring dashboard for Databricks Structured Streaming workloads.

Claude should use this file as authoritative guidance when producing designs, schemas, queries, or implementation details related to streaming observability.

---

# 1. Environment Assumptions

- Platform: Databricks
- Engine: Apache Spark Structured Streaming
- Storage Layer: Delta Lake
- Workloads: Multiple concurrent streaming jobs
- Environment: Enterprise production
- SLAs: Strict latency and availability requirements
- Alerting required
- Data quality monitoring required
- Multi-environment support (dev, staging, prod)

If missing information, assume enterprise-scale production.

---

# 2. Monitoring Objectives

The dashboard must support the following goals:

1. Real-time stream health visibility
2. SLA latency tracking (event time vs processing time)
3. Backpressure detection
4. State store growth monitoring
5. Infrastructure utilization visibility
6. Data quality validation tracking
7. Cost monitoring (DBU consumption)
8. Failure detection and recovery insights

Design decisions should prioritize reliability, standardization, and scalability.

---

# 3. Required Metric Categories

## 3.1 Operational Metrics (Stream Health)

Capture per micro-batch:
- query_id
- query_name
- batch_id
- timestamp
- input_rows_per_second
- processed_rows_per_second
- batch_duration_ms
- watermark
- state_operator_memory_bytes
- state_rows_total
- num_input_rows
- trigger_execution_time_ms

## 3.2 SLA & Latency Metrics

Compute and store:
- event_time_min
- event_time_max
- processing_time
- latency_p50
- latency_p90
- latency_p99
- max_latency
- sla_breach_flag

Latency definition:

processing_time - event_time

## 3.3 Backpressure Indicators

Backpressure exists when:

input_rows_per_second > processed_rows_per_second

Sustained imbalance over multiple batches should trigger alerts.

## 3.4 Data Quality Metrics

Track per batch:
- null_rate_by_column
- duplicate_rate
- dropped_record_count
- schema_change_detected
- late_record_percentage

## 3.5 Error Metrics

Capture:
- exception_type
- exception_message
- failed_batch_id
- termination_reason
- restart_count

## 3.6 Infrastructure Metrics

Track:
- cluster_id
- cpu_usage_percent
- memory_usage_percent
- executor_count
- autoscaling_events
- dbu_consumption

---

# 4. Delta Monitoring Schema

All monitoring tables must be stored in a dedicated schema:

monitoring.streaming_metrics
monitoring.stream_errors
monitoring.data_quality_metrics
monitoring.sla_latency_metrics
monitoring.infrastructure_metrics

Requirements:
- Partition by date
- Include environment column (dev/stage/prod)
- Include workspace_id
- Use append-only design
- Enable table optimization (OPTIMIZE, ZORDER where appropriate)

---

# 5. Architecture Pattern

Preferred Architecture:

Streaming Job
      ↓
StreamingQueryListener
      ↓
Delta Monitoring Tables
      ↓
Databricks SQL Dashboard
      ↓
Alerting Layer (Slack / PagerDuty / Email)

Optional enterprise integration:

Streaming Job
      ↓
Metrics Exporter
      ↓
Prometheus
      ↓
Grafana
      ↓
Incident Management

---

# 6. Dashboard Design Requirements

The dashboard must include:

## Section 1: Stream Overview
- Current status
- Uptime
- Last batch timestamp
- SLA breach indicator

## Section 2: Throughput Trends
- Input vs processed rows/sec
- Batch duration trend
- Watermark delay

## Section 3: Latency & SLA
- P50, P90, P99 latency
- Max latency
- SLA breach count

## Section 4: Backpressure Detection
- Ratio of input to processed
- Sustained imbalance indicator

## Section 5: State Store Health
- Memory usage trend
- State row growth
- Spill detection

## Section 6: Data Quality
- Null rate trends
- Duplicate rate
- Schema drift events

## Section 7: Infrastructure & Cost
- CPU/memory trends
- Executor count
- DBU consumption over time

---

# 7. Alerting Strategy

## Threshold Alerts

Trigger alerts when:
- SLA breach occurs
- Backpressure sustained > N batches
- Batch duration spikes > X%
- Stream terminated
- State memory grows abnormally

## Anomaly Detection

Use rolling averages and deviation detection for:
- Batch duration spikes
- Latency distribution shifts
- Throughput drops

## Escalation

- Warning → Slack
- Critical → PagerDuty
- Failure → Immediate escalation

---

# 8. Scaling & Governance Rules

- Listener logic must be lightweight and non-blocking
- Avoid expensive operations inside onQueryProgress
- Write metrics asynchronously if needed
- Standardize metric names across all streams
- Use idempotent stream restarts
- Maintain consistent tagging (domain, owner, criticality)

For high scale:
- Aggregate metrics periodically
- Archive historical data
- Optimize Delta tables regularly

---

# 9. Best Practices

- Every stream must register a standardized listener
- Every stream must include SLA definitions
- Monitoring tables must never block streaming jobs
- Dashboard must distinguish between real-time and historical views
- Cost monitoring must be tied to workload tagging
- Monitoring design must be environment-aware

---

# 10. Output Expectations for Claude

When generating solutions based on this file:

- Be implementation-ready
- Provide schemas and code where applicable
- Avoid generic advice
- Prioritize enterprise reliability
- Include SQL examples when relevant
- Include PySpark listener examples when relevant
- Include ASCII diagrams when explaining architecture

Assume the reader is a senior data engineer deploying to production.

End of file.

