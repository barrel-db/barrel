# Prometheus Metrics

!!! note "See also: Observability Guide"
    For a complete guide covering tracing, metrics, and logging with exporters, see the [Observability Guide](observability.md).

Barrel DocDB exposes metrics in Prometheus format for monitoring and alerting.

## Endpoint

Metrics are available at:

```
GET /metrics
```

**Example:**
```bash
curl http://localhost:8080/metrics
```

## Available Metrics

### Document Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_doc_operations_total` | Counter | db, operation | Total document operations |
| `barrel_doc_operation_duration_seconds` | Histogram | db, operation | Operation latency |

**Operations:** `put`, `get`, `delete`

**Example:**
```
barrel_doc_operations_total{db="mydb",operation="put"} 1234
barrel_doc_operations_total{db="mydb",operation="get"} 5678
barrel_doc_operation_duration_seconds_bucket{db="mydb",operation="get",le="0.001"} 5000
```

### Query Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_query_operations_total` | Counter | db | Total query operations |
| `barrel_query_duration_seconds` | Histogram | db | Query latency |

**Example:**
```
barrel_query_operations_total{db="mydb"} 500
barrel_query_duration_seconds_bucket{db="mydb",le="0.01"} 450
```

### Replication

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_replication_docs_total` | Counter | source, target | Documents replicated |
| `barrel_replication_errors_total` | Counter | source, target | Replication errors |

**Example:**
```
barrel_replication_docs_total{source="mydb",target="http://remote:8080/mydb"} 10000
barrel_replication_errors_total{source="mydb",target="http://remote:8080/mydb"} 2
```

### HTTP Server

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_http_requests_total` | Counter | method, path, status | HTTP requests |
| `barrel_http_request_duration_seconds` | Histogram | method, path | Request latency |

**Example:**
```
barrel_http_requests_total{method="GET",path="/db/:db/:doc_id",status="200"} 5000
barrel_http_requests_total{method="POST",path="/db/:db/_find",status="200"} 500
```

## Grafana Dashboard

Example queries for common dashboards:

### Request Rate

```promql
rate(barrel_http_requests_total[5m])
```

### Error Rate

```promql
sum(rate(barrel_http_requests_total{status=~"5.."}[5m]))
/
sum(rate(barrel_http_requests_total[5m]))
```

### P99 Latency

```promql
histogram_quantile(0.99, rate(barrel_doc_operation_duration_seconds_bucket[5m]))
```

### Documents per Second

```promql
rate(barrel_doc_operations_total{operation="put"}[5m])
```

### Replication Lag

```promql
increase(barrel_replication_docs_total[1m])
```

## Alerting Examples

### High Error Rate

```yaml
- alert: BarrelHighErrorRate
  expr: |
    sum(rate(barrel_http_requests_total{status=~"5.."}[5m]))
    /
    sum(rate(barrel_http_requests_total[5m])) > 0.01
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High error rate on Barrel DocDB"
```

### Replication Errors

```yaml
- alert: BarrelReplicationErrors
  expr: increase(barrel_replication_errors_total[5m]) > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Replication errors detected"
```

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
