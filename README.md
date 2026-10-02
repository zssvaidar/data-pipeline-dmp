# data-pipeline-dmp

A local, runnable version of an AWS serverless data pipeline and DMP:
an order API emits behavioral events that land in a data lake, a bookmarked
Spark job curates them, and a SQL query builds a customer segment that is
"activated" to downstream systems.

Everything runs on a laptop with Docker. Each component maps one-to-one to
the AWS service it stands in for, so the same design can later move to AWS
(API Gateway + Lambda, Kinesis, Firehose, S3, Glue, Athena).

```
client ─POST /orders─▶ api (Go) ──▶ postgres            (orders: transactional write)
                          │
                          └─event─▶ redpanda ──▶ lander ──▶ minio: raw/behavior-events/year=/month=/day=/
                                                                  │
                     postgres (orders) ──JDBC──▶ etl (PySpark, bookmarked)
                                                                  ▼
                                                 minio: curated/order_events/event_date=/
                                                                  │
                                                     segment (DuckDB SQL)
                                                                  ▼
                                   minio: activation/high_value_customers/dt=/  (+ optional webhook)
```

| Stage | AWS | Local | Code |
|---|---|---|---|
| API | API Gateway + Go Lambda | Go HTTP server | `services/cmd/api`, `services/internal/order` |
| Transactional store | DynamoDB `Orders` | Postgres `orders` | `scripts/init.sql` |
| Event stream | Kinesis `behavior-events` | Redpanda (Kafka API) | `services/internal/events` |
| Stream → lake | Kinesis Firehose | `lander` (Go) | `services/cmd/lander`, `services/internal/lander` |
| Data lake | S3 | MinIO | — |
| Catalog | Glue Crawler | Explicit schema in the job | `etl/jobs/order_events_etl.py` |
| ETL | Glue job + bookmarks | PySpark + file bookmark | `etl/jobs/order_events_etl.py` |
| Query + activation | Athena + activation Lambda | DuckDB | `query/segment.sql`, `query/segment.py` |
| Orchestration | Glue Trigger / Workflow | `make pipeline` | `Makefile` |

## Quick start

Requires Docker with Compose v2. The first build compiles MinIO from source,
which takes a few minutes.

```sh
make up              # start api, lander, redpanda, postgres, minio
make orders N=200    # send sample orders (a few "whale" customers spend big)
                     # wait for the lander to flush (FLUSH_INTERVAL, default 30s)
make pipeline        # run the ETL job, then build + activate the segment
make etl             # run again: "no new raw files" (the bookmark at work)
make down            # stop (make clean also deletes the data volumes)
```

Useful endpoints:

- API: `curl -X POST localhost:8080/orders -d '{"customer_id":"c-1","amount":12.5}'`
- MinIO console: <http://localhost:9001> (minioadmin / minioadmin), bucket `data-lake`
- Postgres: `docker compose exec postgres psql -U pipeline`

Segment settings: `MIN_SPEND` (default 10000), `WINDOW_DAYS` (30),
`WEBHOOK_URL` (POST the segment there), e.g. `MIN_SPEND=5000 make segment`.

## Tests

```sh
make test-go                                  # Go: handler + lander
pip install -r requirements-dev.txt           # needs Java 17+ for PySpark
make test-py                                  # ETL + segment
```

## Design notes

These are the behaviours worth being able to explain.

- **The event never fails the order.** The API writes the order first, then
  produces the event asynchronously and only logs a failure
  (`order.Publisher` has no error return). If the write fails, no event is sent.
- **At-least-once landing.** The lander commits Kafka offsets only after the
  S3 object is written. A crash in between re-lands records, so it can
  duplicate them but never lose them. Batches flush on size (`FLUSH_BYTES`) or
  age (`FLUSH_INTERVAL`), the same as Firehose buffering hints.
- **Job bookmarks.** The ETL job records which raw files it has processed in
  `_state/bookmarks/order_events_etl.json` and reads only new ones. The
  bookmark is committed after the output write, like Glue's `job.commit()`,
  so a failed run is retried rather than skipped.
- **Duplicates are handled downstream.** Because landing and ETL are both
  at-least-once, the job de-duplicates on `event_id` within a batch and the
  segment query de-duplicates on `order_id` across batches.
- **Small files.** Output is repartitioned by `event_date` before writing.
  Without that, the join's 200 shuffle partitions turned 201 rows into 121
  Parquet files.
- **Explicit schema instead of a crawler.** A raw-data schema change fails
  the job loudly instead of silently changing the table.

## Moving to AWS (phase 2)

- `api`: wrap `order.Handler` in a Lambda adapter, implement `Store` with
  DynamoDB and `Publisher` with Kinesis; deploy with SAM.
- `lander`: replace with a Firehose delivery stream (same S3 prefix layout).
- `etl`: run the job on Glue 5 (Spark 3.5); swap the readers for
  `GlueContext.create_dynamic_frame` with `transformation_ctx` to use native
  bookmarks, and read orders from the catalog.
- `segment`: the SQL runs on Athena almost unchanged; the activation step
  becomes a small Lambda.

## Notes

- MinIO no longer publishes container images, so `docker/minio.Dockerfile`
  builds the server from source at a pinned commit.
- Official images are pulled from the `public.ecr.aws/docker/library` mirror
  to avoid Docker Hub rate limits.
