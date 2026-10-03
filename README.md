# data-pipeline-dmp

Site-event ingest: **HTTP API → Lambda → Kafka (Redpanda)**, built with AWS SAM.
A simulator sends realistic site traffic (page views, searches, carts,
purchases, errors and some invalid events) so you can exercise the pipeline.

```
simulator ──POST /events──► API Gateway (HTTP API) ──► Lambda ──► Kafka topic `site-events`
                             └── locally: sam local start-api      └── locally: Redpanda in Docker
```

## Layout

| Path | What |
|---|---|
| `template.yaml` | SAM template: function, HTTP API, log group, alarms, dashboard, canary deploys |
| `samconfig.toml` | Defaults for `sam local` and `sam deploy` |
| `src/site_events/` | Lambda code: `app.py` handler, `site_event.py` validation, `producer.py` Kafka client |
| `tests/` | Unit tests (no Docker or AWS needed) |
| `events/` | Sample API Gateway events for `sam local invoke` |
| `simulator/simulate.py` | Traffic generator (stdlib only) |
| `docker-compose.yml` | Local Redpanda + Redpanda Console |
| `env.local.json` | Env vars for the function when running locally |

## Run locally

Needs Docker, [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
and Python 3.12.

```bash
make up          # Redpanda on the `dmp-local` Docker network, console at http://localhost:8080
make api         # sam build + sam local start-api -> http://127.0.0.1:3000/events
make simulate    # in another terminal: endless simulated traffic (Ctrl+C to stop)
```

One-off checks:

```bash
make invoke                                   # single invoke with events/page_view.json
curl -X POST http://127.0.0.1:3000/events -d '{"event_type":"purchase","user_id":"u1"}'
docker compose exec redpanda rpk topic consume site-events -n 5
```

`sam local` runs the function in the official Lambda container image with the
Lambda Runtime Interface Emulator, attached to the `dmp-local` network so it
reaches the broker at `redpanda:9092`. Locally there is no IAM enforcement and
no CloudWatch; logs and EMF metric lines print to the terminal.

## Unit tests

```bash
pip install -r tests/requirements.txt
make test
```

## API

`POST /events` with JSON:

```json
{"event_type": "add_to_cart", "user_id": "u1", "session_id": "s1",
 "properties": {"sku": "SKU-1001", "qty": 1}}
```

`event_type` is one of `page_view, click, search, add_to_cart, remove_from_cart,
checkout_started, purchase, signup, login, logout, error`. `event_id` and `ts`
are generated when missing. Records are keyed by `session_id` (then `user_id`)
so a session's events stay ordered within a partition.

| Status | Meaning |
|---|---|
| 202 | Written to Kafka; body has `event_id`, `partition`, `offset` |
| 400 | Invalid JSON or unknown `event_type` (nothing written) |
| 502 | Kafka unreachable or didn't ack within ~5s |

## Deploy to AWS

The function needs a Kafka broker it can reach from AWS (for example Redpanda
Cloud or Amazon MSK). Your local Redpanda isn't reachable from AWS.

```bash
sam build
sam deploy --guided     # asks for KafkaBootstrapServers, KafkaSecretArn, AlarmEmail, ...
make simulate EVENTS_URL=<EventsUrl output>
sam delete              # removes everything the stack created
```

Parameters:

- `KafkaSecretArn`: a Secrets Manager secret `{"username": "...", "password": "..."}` for SASL. Leave it empty for no auth.
- `DeploymentType`: `Canary10Percent5Minutes` for prod. New versions take 10% of traffic for 5 minutes and roll back automatically if an alarm fires.
- `AlarmEmail`: subscribes an email to the alarm SNS topic. Confirm the subscription email.

### Health and monitoring

- **Alarms:** `Errors`, `Throttles`, p99 `Duration` > 80% of the timeout, and the custom `KafkaPublishFailures` metric. The function answers 502 instead of crashing when Kafka is down, so `Errors` alone wouldn't catch an outage.
- **Custom metrics** (namespace `SiteEvents`, written with Powertools): `EventsPublished`, `InvalidEvents`, `KafkaPublishFailures`, `KafkaPublishLatency`.
- **Dashboard:** `site-events-<stage>` in CloudWatch.
- **Logs:** structured JSON with `event_id` and `event_type`, kept 14 days. Tail them with `make logs`.
- **Tracing:** X-Ray is enabled.

### Cost

Lambda's always-free tier is 1M requests and 400k GB-seconds a month. The HTTP
API, CloudWatch (alarms, dashboard, logs) and Secrets Manager have small
charges beyond their free tiers. The Kafka cluster is the main cost. A NAT
gateway costs about $30/month even when idle, and is only needed if you put the
function in a VPC that requires internet access.
