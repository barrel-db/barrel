# Prometheus Metrics

> #### See also: Observability Guide
>
> For a complete guide covering tracing, metrics, and logging with exporters, see the [Observability Guide](observability.md).

Barrel DocDB records metrics in-process using the [instrument](https://github.com/benoitc/instrument) library and can render them in Prometheus text format.

## Reading Metrics

barrel_docdb has no built-in HTTP server. Render the current metrics in Prometheus text format in-process:

```erlang
Text = barrel_metrics:export_text().
%% Prometheus exposition format, ready to serve or log
```

To expose metrics over HTTP for a Prometheus scrape, run the `barrel_server` app, which serves this output on its metrics endpoint. See the umbrella REST server guide (`docs/guides/rest-server.md`). Metrics can also be pushed to an OTLP collector; see [Observability](observability.md).

## Available Metrics

### Document Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_doc_operations` | Counter | db, operation | Total document operations |
| `barrel_doc_operation_duration_seconds` | Histogram | db, operation | Operation latency |

**Operations:** `put`, `get`, `delete`

**Example:**
```
barrel_doc_operations{db="mydb",operation="put"} 1234
barrel_doc_operations{db="mydb",operation="get"} 5678
barrel_doc_operation_duration_seconds_bucket{db="mydb",operation="get",le="0.001"} 5000
```

### Query Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_query_operations` | Counter | db | Total query operations |
| `barrel_query_duration_seconds` | Histogram | db | Query latency |
| `barrel_query_results_count` | Histogram | db | Results per query |

**Example:**
```
barrel_query_operations{db="mydb"} 500
barrel_query_duration_seconds_bucket{db="mydb",le="0.01"} 450
```

### Replication

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_replication_docs` | Counter | direction | Documents replicated |
| `barrel_replication_errors` | Counter | task_id | Replication errors |
| `barrel_replication_lag_seconds` | Gauge | task_id | Replication lag |
| `barrel_replication_active` | Gauge | task_id | Active status (1/0) |

**Example:**
```
barrel_replication_docs{direction="push"} 10000
barrel_replication_errors{task_id="rep-123"} 2
```

### Storage

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_db_documents_total` | Gauge | db | Document count |
| `barrel_db_size_bytes` | Gauge | db | Database size |
| `barrel_db_attachments_total` | Gauge | db | Attachment count |

> #### HTTP request metrics
>
> Metrics for HTTP requests (method, path, status, latency) are produced by `barrel_server`, not barrel_docdb, since barrel_docdb has no HTTP surface of its own.

## Grafana Dashboard

Example queries for common dashboards:

### Document Operation Rate

```promql
rate(barrel_doc_operations[5m])
```

### P99 Document Latency

```promql
histogram_quantile(0.99, rate(barrel_doc_operation_duration_seconds_bucket[5m]))
```

### Documents per Second

```promql
rate(barrel_doc_operations{operation="put"}[5m])
```

### Query Throughput

```promql
rate(barrel_query_operations[5m])
```

### Replication Throughput

```promql
increase(barrel_replication_docs[1m])
```

## Alerting Examples

### High Latency

```yaml
- alert: BarrelHighLatency
  expr: |
    histogram_quantile(0.99, rate(barrel_doc_operation_duration_seconds_bucket[5m])) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High P99 latency on Barrel DocDB"
```

### Replication Errors

```yaml
- alert: BarrelReplicationErrors
  expr: increase(barrel_replication_errors[5m]) > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Replication errors detected"
```

## Configuration

Metrics are always recorded internally. Set the exporter to `none` (the
default) to suppress external export, or to `console` / `otlp` to push them
out. See [Observability](observability.md) for details.

```erlang
{barrel_docdb, [
    {metrics, [
        {exporter, none}  %% or console | otlp
    ]}
]}.
```
