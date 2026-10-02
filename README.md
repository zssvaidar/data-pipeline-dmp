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
make test-go                                  # Go: handler, lander, Lambda adapter, activation
pip install -r requirements-dev.txt           # needs Java 17+ for PySpark
make test-py                                  # ETL, Glue entry point (stubbed awsglue), segment
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

## On AWS (phase 2)

`infra/template.yaml` (AWS SAM) deploys the same design as managed services.
The Go handler, the lander's raw-zone layout, the Spark transform and the
segment SQL are all shared with the local stack.

| Stage | Resource | Code |
|---|---|---|
| API | HTTP API -> Go Lambda (arm64) | `services/cmd/lambda-api`, `internal/lambdahttp` |
| Orders | DynamoDB on-demand, PITR on | `internal/order/dynamo.go` |
| Events | Kinesis (1 shard) -> Firehose -> S3 `raw/` | `internal/events/kinesis.go` |
| Catalog | Glue crawler, nightly, new folders only | template |
| ETL | Glue 5.0 job, native bookmarks, catalog-updating sink | `etl/jobs/glue_order_events.py` |
| Orchestration | Conditional trigger: crawler SUCCEEDED -> job | template |
| Segment | EventBridge (job SUCCEEDED) -> Lambda -> Athena -> S3 + SNS | `services/cmd/lambda-activate`, `internal/activate` |
| Ops | Athena scan cap, S3 lifecycle, API error alarm, ETL failure alert | template |

### Deploy

Requires an AWS account, credentials, the AWS CLI, the SAM CLI and Go.

```sh
make lint-aws                         # offline template check
make aws-deploy                       # sam build + deploy + upload Glue scripts
make aws-orders N=200                 # send orders to the deployed API
                                      # wait for the Firehose buffer (5 min by default)
make aws-pipeline                     # crawler -> ETL -> activation, prints the segment
make aws-destroy                      # delete the stack and all data
```

Options: `STACK_NAME` (default `dmp`), `AWS_REGION`, and template parameters
passed to the deploy script, e.g.
`./infra/deploy.sh FirehoseBufferSeconds=60 MinSpend=5000`.
Subscribe to the `SegmentTopicArn` and `AlertsTopicArn` outputs (email, SQS,
Lambda) to receive segment and failure notifications.

### Cost

At demo volume the stack costs roughly **$12–15 a month while it exists**,
mostly the provisioned Kinesis shard (~$11/month). Per pipeline run, the
crawler is ~$0.07 (10-minute minimum) and a short Glue job ~$0.05. Lambda,
DynamoDB, Firehose, S3 and Athena are cents. Run `make aws-destroy` when done.

### Differences from the local stack

- The Lambda sends the Kinesis record synchronously with a 500 ms timeout:
  a Lambda is frozen after it returns, so a background send could be lost.
- The Glue job reads orders with a DynamoDB scan capped at 50% of read
  capacity. At scale, switch to DynamoDB export to S3.
- Native Glue bookmarks replace the local processed-file list; the sink also
  registers new curated partitions, so no second crawler is needed.

## Notes

- MinIO no longer publishes container images, so `docker/minio.Dockerfile`
  builds the server from source at a pinned commit.
- Official images are pulled from the `public.ecr.aws/docker/library` mirror
  to avoid Docker Hub rate limits.
