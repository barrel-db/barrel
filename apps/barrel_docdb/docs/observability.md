# Observability

Barrel DocDB provides observability through three pillars using the [instrument](https://github.com/benoitc/instrument) library:

- **Tracing** - Distributed traces with OpenTelemetry-compatible spans
- **Metrics** - Prometheus-compatible metrics for monitoring
- **Logging** - Structured logging with trace context correlation

All three signals are automatically correlated via trace context, enabling end-to-end debugging across distributed requests.

## Quick Start

### Enable Console Tracing (Development)

```erlang
%% In sys.config
{barrel_docdb, [
    {tracing, [
        {enabled, true},
        {exporter, console}
    ]}
]}
```

### Enable OTLP Export for All Signals (Production)

```erlang
%% In sys.config
{barrel_docdb, [
    %% Tracing - spans to OTLP
    {tracing, [
        {enabled, true},
        {exporter, otlp},
        {otlp_endpoint, "http://localhost:4318"}
    ]},
    %% Metrics - push to OTLP (in addition to /metrics endpoint)
    {metrics, [
        {exporter, otlp},
        {otlp_endpoint, "http://localhost:4318"},
        {export_interval, 60000}  %% ms, default 60s
    ]},
    %% Logging - push to OTLP
    {logging, [
        {exporter, otlp},
        {otlp_endpoint, "http://localhost:4318"}
    ]}
]}
```

### Scrape Metrics (Pull Model)

```bash
curl http://localhost:8080/metrics
```

---

## Tracing

Barrel DocDB implements OpenTelemetry-compatible distributed tracing, enabling you to trace requests across HTTP handlers, document operations, queries, replication, and cross-node communication.

### Configuration

Add tracing configuration to your `sys.config`:

```erlang
{barrel_docdb, [
    {tracing, [
        {enabled, true},           %% Enable/disable tracing (default: true)
        {exporter, console},       %% Exporter type (see below)
        {console_format, text},    %% For console: text | json
        {otlp_endpoint, "http://localhost:4318"}  %% For OTLP
    ]}
]}
```

### Exporters

#### No Export (Default)

Tracing is enabled internally but spans are not exported. Useful when you only need trace context correlation in logs.

```erlang
{tracing, [{enabled, true}, {exporter, none}]}
```

#### Console Exporter

Prints spans to stdout. Useful for development and debugging.

```erlang
{tracing, [
    {enabled, true},
    {exporter, console},
    {console_format, text}  %% text | json
]}
```

**Example output (text format):**
```
[span] GET /db/mydb/doc/doc123 (12.5ms)
  trace_id: 0af7651916cd43dd8448eb211c80319c
  span_id: b7ad6b7169203331
  attributes:
    http.request.method: GET
    url.path: /db/mydb/doc/doc123
    http.response.status_code: 200
    db.system.name: barrel
```

#### OTLP Exporter

Sends spans to an OpenTelemetry Collector or compatible backend (Jaeger, Zipkin, Datadog, etc.).

```erlang
{tracing, [
    {enabled, true},
    {exporter, otlp},
    {otlp_endpoint, "http://localhost:4318"}
]}
```

**Docker Compose example with Jaeger:**

Tracing is configured in `sys.config`. Mount a custom config into the
container and point barrel at it.

```yaml
services:
  barrel:
    image: barrel/barrel_docdb
    volumes:
      - ./sys.config:/app/releases/0.6.2/sys.config:ro

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # Jaeger UI
      - "4318:4318"    # OTLP HTTP
```

With `sys.config`:

```erlang
[{barrel_docdb, [
    {tracing, [
        {enabled, true},
        {exporter, otlp},
        {otlp_endpoint, "http://jaeger:4318"}
    ]}
]}].
```

#### Custom Exporter

Use any module implementing the `instrument_exporter` behaviour:

```erlang
{tracing, [
    {enabled, true},
    {exporter, {my_custom_exporter, #{option1 => value1}}}
]}
```

### Trace Context Propagation

Barrel DocDB automatically propagates trace context using W3C Trace Context headers (`traceparent`, `tracestate`).

**Incoming requests:** Trace context is extracted from HTTP headers and attached to the request span.

**Outgoing requests:** During replication, trace context is injected into HTTP headers for cross-node correlation.

**Response headers:** The `traceparent` header is included in responses for client-side correlation.

### Instrumented Operations

| Component | Span Name | Kind | Attributes |
|-----------|-----------|------|------------|
| HTTP Handler | `{METHOD} {PATH}` | server | http.request.method, url.path, http.response.status_code |
| Document Put | `put {db}` | internal | db.system.name, db.namespace, db.operation.name, db.document.id |
| Document Get | `get {db}` | internal | db.system.name, db.namespace, db.operation.name, db.document.id |
| Document Delete | `delete {db}` | internal | db.system.name, db.namespace, db.operation.name, db.document.id |
| Batch Put | `put_batch {db}` | internal | db.batch_size |
| Batch Get | `get_batch {db}` | internal | db.batch_size |
| Query | `query {db}` | internal | db.response.returned_rows |
| Query Compile | `query_compile` | internal | - |
| Query Execute | `query_execute {db}` | internal | - |
| Changes Feed | `changes {db}` | internal | - |
| Replication | `replication` | internal | replication.changes_count, replication.source_transport, replication.target_transport |
| Replication Task | `rep_task` | internal | replication.task_id, replication.direction, replication.mode |

### Semantic Conventions

Barrel DocDB follows [OpenTelemetry Database Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/database/):

| Attribute | Type | Description |
|-----------|------|-------------|
| `db.system.name` | string | Always `"barrel"` |
| `db.namespace` | string | Database name |
| `db.operation.name` | string | Operation type (get, put, delete, query) |
| `db.document.id` | string | Document identifier |
| `db.batch_size` | int | Number of documents in batch operation |
| `db.response.returned_rows` | int | Number of results returned |
| `db.view.name` | string | View identifier |

---

## Metrics

Barrel DocDB supports two modes for metrics export:
- **Pull model** - Prometheus scrapes the `/metrics` endpoint
- **Push model** - Metrics are pushed to an OTLP collector

### Configuration

```erlang
{barrel_docdb, [
    {metrics, [
        {exporter, none},              %% none | otlp | console
        {otlp_endpoint, "http://localhost:4318"},
        {export_interval, 60000},      %% Push interval in ms (default: 60s)
        {otlp_headers, #{}},           %% Optional auth headers
        {otlp_compression, none}       %% none | gzip
    ]}
]}
```

### Pull Model (Prometheus Scraping)

Metrics are always available at the `/metrics` endpoint regardless of push configuration:

```bash
curl http://localhost:8080/metrics
```

### Available Metrics

#### Document Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_doc_operations` | Counter | db, operation | Total document operations |
| `barrel_doc_operation_duration_seconds` | Histogram | db, operation | Operation latency |

**Operations:** `put`, `get`, `delete`

**Example:**
```prometheus
barrel_doc_operations{db="mydb",operation="put"} 1234
barrel_doc_operation_duration_seconds_bucket{db="mydb",operation="get",le="0.01"} 5000
```

#### Query Operations

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_query_operations` | Counter | db | Total query operations |
| `barrel_query_duration_seconds` | Histogram | db | Query latency |
| `barrel_query_results_count` | Histogram | db | Results per query |

#### Replication

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_replication_docs` | Counter | direction | Documents replicated |
| `barrel_replication_errors` | Counter | task_id | Replication errors |
| `barrel_replication_lag_seconds` | Gauge | task_id | Replication lag |
| `barrel_replication_active` | Gauge | task_id | Active status (1/0) |

#### Storage

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_db_documents_total` | Gauge | db | Document count |
| `barrel_db_size_bytes` | Gauge | db | Database size |
| `barrel_db_attachments_total` | Gauge | db | Attachment count |

#### HTTP Server

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `barrel_http_requests` | Counter | method, path, status | HTTP requests |
| `barrel_http_request_duration_seconds` | Histogram | method, path | Request latency |

### Prometheus Configuration (Pull)

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'barrel_docdb'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /metrics
    scrape_interval: 15s
```

### Push Model (OTLP)

To push metrics directly to an OpenTelemetry Collector:

```erlang
{barrel_docdb, [
    {metrics, [
        {exporter, otlp},
        {otlp_endpoint, "http://otel-collector:4318"}
    ]}
]}
```

**With authentication:**

```erlang
{barrel_docdb, [
    {metrics, [
        {exporter, otlp},
        {otlp_endpoint, "https://otlp.vendor.com:4318"},
        {otlp_headers, #{
            <<"Authorization">> => <<"Bearer your-api-key">>
        }},
        {otlp_compression, gzip}
    ]}
]}
```

**OpenTelemetry Collector configuration for metrics:**

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
  # Or send to a metrics backend
  otlphttp:
    endpoint: "https://metrics-backend.example.com"

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

### Grafana Dashboard Queries

#### Request Rate
```promql
rate(barrel_http_requests[5m])
```

#### Error Rate
```promql
sum(rate(barrel_http_requests{status=~"5.."}[5m]))
/
sum(rate(barrel_http_requests[5m]))
```

#### P99 Document Latency
```promql
histogram_quantile(0.99, rate(barrel_doc_operation_duration_seconds_bucket[5m]))
```

#### Documents per Second
```promql
rate(barrel_doc_operations{operation="put"}[5m])
```

#### Query Throughput
```promql
rate(barrel_query_operations[5m])
```

### Alerting Examples

```yaml
# Prometheus alerting rules
groups:
  - name: barrel_docdb
    rules:
      - alert: BarrelHighErrorRate
        expr: |
          sum(rate(barrel_http_requests{status=~"5.."}[5m]))
          / sum(rate(barrel_http_requests[5m])) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate on Barrel DocDB"

      - alert: BarrelHighLatency
        expr: |
          histogram_quantile(0.99, rate(barrel_doc_operation_duration_seconds_bucket[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High P99 latency on Barrel DocDB"

      - alert: BarrelReplicationErrors
        expr: increase(barrel_replication_errors[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Replication errors detected"
```

---

## Logging

Barrel DocDB uses Erlang's built-in `logger` module with automatic trace context enrichment. Logs can be:
- Written to stdout (default Erlang logger)
- Written to files with rotation
- Pushed to an OTLP collector for centralized logging

### Configuration

```erlang
{barrel_docdb, [
    {logging, [
        {exporter, none},              %% none | console | file | otlp

        %% File exporter options
        {file_path, "/var/log/barrel.log"},
        {file_format, text},           %% text | json
        {file_max_size, 10485760},     %% 10MB, 0 = unlimited
        {file_max_files, 5},           %% Number of rotated files
        {file_compress, false},        %% Compress rotated files

        %% OTLP exporter options
        {otlp_endpoint, "http://localhost:4318"},
        {otlp_headers, #{}},           %% Optional auth headers
        {otlp_compression, none}       %% none | gzip
    ]}
]}
```

### Standard Output (Default)

Configure logging in `sys.config`:

```erlang
{kernel, [
    {logger_level, info},
    {logger, [
        {handler, default, logger_std_h, #{
            level => info,
            formatter => {logger_formatter, #{
                template => [time, " [", level, "] ", pid, " ", msg, "\n"]
            }}
        }}
    ]}
]}
```

### Trace Context in Logs

When tracing is enabled, log messages are automatically enriched with trace context:

```erlang
%% In your code
logger:info("Processing document ~s", [DocId])
```

**Output with trace context:**
```
2024-01-15T10:30:45.123Z [info] <0.234.0> Processing document doc123 trace_id=0af7651916cd43dd8448eb211c80319c span_id=b7ad6b7169203331
```

### JSON Logging for Production

For structured logging (recommended for production):

```erlang
{kernel, [
    {logger_level, info},
    {logger, [
        {handler, default, logger_std_h, #{
            level => info,
            formatter => {logger_formatter, #{
                template => [msg],
                single_line => true
            }},
            config => #{
                type => standard_io
            }
        }}
    ]}
]}
```

### Log Levels

| Level | Description |
|-------|-------------|
| `debug` | Detailed debugging information |
| `info` | General operational messages |
| `notice` | Normal but significant events |
| `warning` | Warning conditions |
| `error` | Error conditions |
| `critical` | Critical conditions |
| `alert` | Action must be taken immediately |
| `emergency` | System is unusable |

### File Exporter

Write logs to files with automatic rotation:

```erlang
{barrel_docdb, [
    {logging, [
        {exporter, file},
        {file_path, "/var/log/barrel/app.log"},
        {file_format, json},           %% text | json
        {file_max_size, 52428800},     %% 50MB
        {file_max_files, 10},          %% Keep 10 rotated files
        {file_compress, true}          %% Compress rotated files (.gz)
    ]}
]}
```

**File rotation behavior:**
- When `file_max_size` is reached, files rotate: `app.log` -> `app.log.1` -> `app.log.2` -> ...
- If `file_compress` is true, rotated files become `app.log.1.gz`, etc.
- Set `file_max_size` to 0 to disable rotation

**JSON format output:**
```json
{"timeUnixNano":"1705312245123456000","severityText":"INFO","body":{"stringValue":"Processing document doc123"},"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331"}
```

**Text format output:**
```
[2024-01-15T10:30:45.123456Z] INFO [trace_id=0af7651916cd43dd span_id=b7ad6b7169203331] Processing document doc123
```

### Push Model (OTLP)

To push logs directly to an OpenTelemetry Collector:

```erlang
{barrel_docdb, [
    {logging, [
        {exporter, otlp},
        {otlp_endpoint, "http://otel-collector:4318"}
    ]}
]}
```

**With authentication:**

```erlang
{barrel_docdb, [
    {logging, [
        {exporter, otlp},
        {otlp_endpoint, "https://logs.vendor.com:4318"},
        {otlp_headers, #{
            <<"Authorization">> => <<"Bearer your-api-key">>
        }},
        {otlp_compression, gzip}
    ]}
]}
```

**OpenTelemetry Collector configuration for logs:**

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  loki:
    endpoint: "http://loki:3100/loki/api/v1/push"
  # Or send to another backend
  elasticsearch:
    endpoints: ["http://elasticsearch:9200"]

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [loki]
```

### Correlating Logs with Traces

Use the trace ID from logs to find the corresponding trace in your tracing backend:

1. Find the error in logs:
   ```
   2024-01-15T10:30:45.123Z [error] Document conflict trace_id=0af7651916cd43dd8448eb211c80319c
   ```

2. Search for the trace ID in Jaeger/Zipkin/etc.

3. View the full trace to understand the request flow

---

## Complete Example Setup

### Docker Compose with Full Observability

```yaml
version: '3.8'

services:
  barrel:
    image: barrel/barrel_docdb
    ports:
      - "8080:8080"
    volumes:
      - barrel-data:/data
      - ./sys.config:/etc/barrel/sys.config

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4318:4318"

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  loki:
    image: grafana/loki:latest
    ports:
      - "3100:3100"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin

volumes:
  barrel-data:
```

### Barrel Configuration (sys.config)

```erlang
%% sys.config - Full OTLP export for all signals
[
    {barrel_docdb, [
        {data_dir, "/data/barrel"},
        {http_port, 8080},

        %% Tracing - push spans to OTLP
        {tracing, [
            {enabled, true},
            {exporter, otlp},
            {otlp_endpoint, "http://otel-collector:4318"}
        ]},

        %% Metrics - push to OTLP (also available via /metrics)
        {metrics, [
            {exporter, otlp},
            {otlp_endpoint, "http://otel-collector:4318"},
            {export_interval, 30000}
        ]},

        %% Logging - push to OTLP
        {logging, [
            {exporter, otlp},
            {otlp_endpoint, "http://otel-collector:4318"}
        ]}
    ]},
    {kernel, [
        {logger_level, info}
    ]}
].
```

### OpenTelemetry Collector Configuration

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024

exporters:
  # Traces to Jaeger
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

  # Metrics to Prometheus
  prometheus:
    endpoint: "0.0.0.0:8889"

  # Logs to Loki
  loki:
    endpoint: "http://loki:3100/loki/api/v1/push"

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheus]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [loki]
```

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  # Scrape barrel directly (pull model)
  - job_name: 'barrel_docdb'
    static_configs:
      - targets: ['barrel:8080']
    metrics_path: /metrics

  # Or scrape from OTEL collector (if using push model)
  - job_name: 'otel-collector'
    static_configs:
      - targets: ['otel-collector:8889']
```

---

## Programmatic Access

### Recording Custom Spans

```erlang
%% Create a custom span for your application logic
barrel_trace:with_db_span(my_operation, <<"mydb">>, fun() ->
    %% Your code here
    barrel_trace:set_attribute(<<"custom.attribute">>, <<"value">>),
    do_work()
end).

%% With extra attributes
barrel_trace:with_db_span(my_operation, <<"mydb">>, #{
    <<"custom.key">> => <<"value">>
}, fun() ->
    do_work()
end).
```

### Recording Errors

```erlang
barrel_trace:with_db_span(risky_operation, <<"mydb">>, fun() ->
    case do_something() of
        {ok, Result} -> Result;
        {error, Reason} ->
            barrel_trace:record_error(Reason),
            {error, Reason}
    end
end).
```

### Adding Events

```erlang
barrel_trace:with_db_span(batch_process, <<"mydb">>, fun() ->
    lists:foreach(fun(Item) ->
        process_item(Item),
        barrel_trace:add_event(<<"item.processed">>, #{
            <<"item.id">> => Item
        })
    end, Items)
end).
```

### Recording Custom Metrics

```erlang
%% Increment a counter
barrel_metrics:inc_doc_ops(<<"mydb">>, put),

%% Record a histogram observation
barrel_metrics:observe_doc_latency(<<"mydb">>, get, 15),  %% 15ms

%% Set a gauge
barrel_metrics:set_db_docs(<<"mydb">>, 1000).
```
