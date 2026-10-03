# Observability plan: logs, metrics and traces

How to add logging, metrics and tracing to the local pipeline with
**OpenTelemetry** (instrumentation), **Grafana Alloy** (collector) and
**Grafana** (UI), storing data in **Loki** (logs), **Prometheus** (metrics)
and **Tempo** (traces).

## What exists today

| Component | Logs | Metrics | Traces |
|---|---|---|---|
| `api` (Go) | JSON via `slog`, no request context | none | none |
| `lander` (Go) | JSON via `slog` (`landed batch`, `flush`, `skipping record`) | none | none |
| `etl` (PySpark) | plain-text `logging`, Spark log4j at WARN | none | none |
| `segment` (Python) | plain-text `logging` | none | none |
| Redpanda, Postgres, MinIO | container stdout | built-in endpoints not scraped | none |

## Target architecture

```
 api ──────┐  OTLP (traces, metrics)
 lander ───┤ ─────────────────────────┐
 etl ──────┤                          ▼
 segment ──┘                 ┌─────────────────┐ ──► Tempo       (traces)
                             │  Grafana Alloy  │ ──► Prometheus  (metrics, remote_write)
 container stdout ──────────►│                 │ ──► Loki        (logs)
 (docker socket)             └─────────────────┘
 redpanda :9644, minio, postgres ──scrape──┘
                                                     Grafana reads all three
```

- All code sends **traces and metrics over OTLP** to Alloy (`alloy:4317`).
- **Logs stay on stdout.** Alloy reads container logs from the Docker socket,
  so services do not ship logs themselves.
- The observability services go in `docker-compose.yml` under a new `obs`
  profile, so `make up` keeps working without them.
- Prometheus is the metrics store locally. Mimir accepts the same
  `remote_write`, so it can replace Prometheus later by changing one URL in
  the Alloy config.

## Conventions (all services)

Resource attributes, set through `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES`:

- `service.name`: `api`, `lander`, `etl`, `segment`
- `deployment.environment`: `local`
- `service.version`: git SHA, passed in at build time

Log fields (JSON, one object per line):

| Field | Example | Notes |
|---|---|---|
| `time`, `level`, `msg` | | `slog` defaults; Python formatter uses the same names |
| `service` | `lander` | also becomes the Loki label |
| `trace_id`, `span_id` | | added when the log call has a span in its context |
| `run_id` | `etl-20261003T020000Z` | batch jobs only |
| domain fields | `order_id`, `key`, `records`, `rows` | keep them in the body, **never as labels** |

Loki labels: `service`, `level`, `env`, `container` only.

Metric names follow OTel semantic conventions where one exists (for example
`http.server.request.duration`) and use a `dmp.` prefix otherwise.

## Phase 0: observability stack (foundation)

1. `observability/alloy/config.alloy`:
   - `otelcol.receiver.otlp` → batch → Tempo (traces) and Prometheus
     (metrics, via `otelcol.exporter.prometheus` → `prometheus.remote_write`)
   - `discovery.docker` + `loki.source.docker` → `loki.process` (parse JSON,
     promote `level`) → Loki
   - `prometheus.scrape` for Redpanda `:9644/public_metrics` and MinIO
     `/minio/v2/metrics/cluster`; `prometheus.exporter.postgres` for Postgres
2. Loki, Tempo and Prometheus in single-binary mode with local volumes.
   Prometheus runs with `--web.enable-remote-write-receiver`.
3. Grafana with provisioned data sources and the links between them:
   - Loki derived field `trace_id` → Tempo
   - Tempo "trace to logs" → Loki, "trace to metrics" → Prometheus
   - Tempo metrics-generator on (span metrics and service graph)
4. `make obs-up` / `make obs-down`; Grafana on `localhost:3000`.

Done when: Grafana shows container logs for every service and Redpanda/MinIO
metrics, with no code changes yet.

## Phase 1: logging

**Go (`api`, `lander`)**
- Add a small shared package `internal/telemetry` with `NewLogger(service)`:
  a `slog.JSONHandler` wrapped by a handler that reads the span from the
  context and adds `trace_id` / `span_id`. (Or use the `otelslog` bridge.)
- Switch log calls on request and batch paths to the context form
  (`log.InfoContext(ctx, ...)`, `log.ErrorContext(ctx, ...)`) so the IDs are
  attached.
- Add useful fields that are missing today: `order_id` on
  `create order` errors, `bytes` on `landed batch`.

**Python (`etl`, `segment`)**
- Replace `logging.basicConfig(format=...)` with a JSON formatter
  (same field names as Go) and a `run_id` generated at job start.
- Add `opentelemetry-instrumentation-logging` so `trace_id` / `span_id`
  are injected into each record.
- Keep Spark's log4j at WARN; its output is collected as-is.

Done when: every log line in Loki is JSON with `service`, `level` and, inside
a request or run, `trace_id`.

## Phase 2: metrics

All application metrics use the OTel metrics SDK and are pushed over OTLP.
Pushing (rather than Prometheus scraping) matters for `etl` and `segment`,
which exit when the run ends; they must flush the meter provider before exit.

**`api`**
- `http.server.request.duration` (histogram, by route and status) from `otelhttp`
- `dmp.orders.created` (counter)
- `dmp.events.published` (counter, `result=ok|error`) incremented in the
  Kafka produce callback, which is the only place a publish failure is
  visible today

**`lander`**
- `dmp.lander.records.consumed`, `dmp.lander.records.skipped` (invalid JSON)
- `dmp.lander.batches.landed`, `dmp.lander.batch.records`,
  `dmp.lander.batch.bytes` (histograms), `dmp.lander.flush.errors`
- `dmp.lander.last_flush.timestamp` (gauge): basis for the freshness alert
- `dmp.lander.buffer.age` (gauge)
- Consumer lag: the `kprom` plugin for franz-go, or Redpanda's own
  consumer-group lag metrics from `public_metrics`

**`etl`** (one data point set per run, labelled `job=order_events_etl`)
- `dmp.etl.run.duration`, `dmp.etl.runs` (`result=success|failure|noop`)
- `dmp.etl.files.processed`, `dmp.etl.rows.written`
- `dmp.etl.duplicates.dropped` (rows before minus after `dropDuplicates`)
- `dmp.etl.orders.not_found` (rows with `order_found = false`): a data
  quality signal for events whose order is missing in Postgres
- `dmp.etl.last_success.timestamp`

**`segment`**
- `dmp.segment.customers` (gauge), `dmp.segment.run.duration`
- `dmp.segment.webhook.requests` (`status`)
- `dmp.segment.last_success.timestamp`

**Alert rules** (Grafana alerting, provisioned from files)

| Alert | Condition |
|---|---|
| API errors | 5xx rate > 1% for 5 min |
| Events not published | `dmp.events.published{result="error"}` increases |
| Lander stalled | consumer lag > 0 and last flush older than 3 × `FLUSH_INTERVAL` |
| Records rejected | `dmp.lander.records.skipped` increases |
| ETL stale | last successful run older than 26 h |
| ETL failed | `dmp.etl.runs{result="failure"}` increases |
| Orphan events | `orders.not_found / rows.written` > 1% |
| Empty segment | `dmp.segment.customers == 0` |

## Phase 3: tracing

The pipeline needs **two kinds of traces**:

1. **Live path, one trace per order:**
   `POST /orders` → Postgres insert → Kafka produce → lander consume → MinIO put
2. **Batch path, one trace per pipeline run:**
   `make pipeline` → ETL steps → segment steps → webhook

They are joined by **attributes**, not by parent/child links: the lander puts
the object key it wrote on its span, and the ETL puts the keys it read on its
span. Searching by object key in Tempo finds both sides.

**`api`**
- Wrap the mux with `otelhttp.NewHandler`.
- Add the `otelpgx` tracer to the pgx pool config: one span per SQL statement.
- Add the `kotel` plugin to the franz-go client: it starts a producer span and
  writes `traceparent` into the Kafka record headers. `Publish` already
  receives `context.WithoutCancel(r.Context())`, which keeps the span context.
- Span attributes: `order.id`, `customer.id` (fine on spans, not on metrics).

**`lander`**
- `kotel` on the consumer side extracts the context from each record.
- One span per flush (`lander.flush`) with **span links** to the records it
  contains (capped, e.g. the first 128), plus `s3.key`, `records`, `bytes`.
  A batch holds many orders, so links are used instead of a single parent.
- `otelaws` middleware on the S3 client gives a child span for `PutObject`.

**`etl`**
- Root span `etl.run` (`run_id`, `files.new`), children:
  `bookmark.load`, `list_files`, `read_and_enrich`, `write_curated`,
  `bookmark.commit`.
- Spark evaluates lazily: the real work happens in `count()` and `write`,
  so put spans around those actions. A span around a transformation alone
  measures almost nothing.
- Record the processed raw keys as a span event (the list can be long, so not
  as an attribute).

**`segment`**
- Root span `segment.run`, children `build_segment`, `activate.write`,
  `activate.webhook`. `opentelemetry-instrumentation-urllib` sends
  `traceparent` to the webhook, so a traced receiver continues the trace.

**One trace per `make pipeline`**
- The Makefile generates a `TRACEPARENT` and passes it to both job containers.
  Each job reads it at start (`TraceContextTextMapPropagator().extract`)
  and makes its root span a child of it, so ETL and segment appear as one
  trace.

**Sampling**
- Locally: keep everything.
- Later: parent-based ratio sampling for `api` traffic; batch runs always kept.

## Phase 4: dashboards

One provisioned dashboard, "Pipeline overview", top to bottom in data-flow order:

1. API: request rate, error rate, p95 latency, events published vs. failed
2. Stream: consumer lag, records consumed, records skipped
3. Lander: batches landed, batch size, time since last flush
4. ETL: last run result and duration, rows written, duplicates,
   orders not found, time since last success
5. Segment: customers in segment, last run, webhook status
6. Recent errors: Loki panel `{level="ERROR"}` with trace links

## Phase 5: tests

- Go: the existing handler and lander tests keep their fakes (the
  `Store`, `Publisher` and `Sink` interfaces do not change). New tests use
  `tracetest.NewSpanRecorder()` and the SDK's manual metric reader to check
  span names, attributes, links and counters.
- Python: `InMemorySpanExporter` and `InMemoryMetricReader` in the existing
  pytest suites to check the run span tree and run metrics.
- End to end: `make obs-up up orders pipeline`, then query Tempo for one
  order trace and one pipeline-run trace through Grafana's API.

## Order of work

| Step | Scope | Size |
|---|---|---|
| 1 | Phase 0: stack, Alloy config, Grafana provisioning | M |
| 2 | Phase 1: JSON logs + `trace_id` field everywhere | S |
| 3 | Phase 3 for `api` + `lander` (live path) | M |
| 4 | Phase 2 for `api` + `lander` | S |
| 5 | Phase 3 + Phase 2 for `etl` + `segment` (batch path, `TRACEPARENT`) | M |
| 6 | Phase 4 dashboard + alert rules | S |
| 7 | Phase 5 tests throughout; end-to-end check last | S |

Logs and tracing come before metrics because the live-path trace answers the
most common question first ("where did this order's event go?"), and many
useful metrics (span metrics, service graph) then come from Tempo for free.
