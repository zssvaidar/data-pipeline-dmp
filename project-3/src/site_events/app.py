"""Site-event ingest Lambda: validate the event, publish it to Kafka, answer like an API."""
import json
import os
import time

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit

from site_event import InvalidEvent, build_record, parse_payload, partition_key
from producer import get_producer

TOPIC = os.getenv("KAFKA_TOPIC", "site-events")
FLUSH_TIMEOUT_S = float(os.getenv("KAFKA_FLUSH_TIMEOUT_S", "8"))

logger = Logger()
metrics = Metrics()


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def publish(record):
    """Send one record and wait for the broker ack. Returns (partition, offset)."""
    result = {}

    def on_delivery(err, msg):
        result["err"] = err
        if err is None:
            result["partition"] = msg.partition()
            result["offset"] = msg.offset()

    producer = get_producer()
    producer.produce(
        TOPIC,
        key=partition_key(record).encode(),
        value=json.dumps(record).encode(),
        on_delivery=on_delivery,
    )
    producer.flush(FLUSH_TIMEOUT_S)

    if result.get("err") is not None:
        raise RuntimeError(str(result["err"]))
    if "offset" not in result:
        raise TimeoutError(f"no delivery report within {FLUSH_TIMEOUT_S}s")
    return result["partition"], result["offset"]


@logger.inject_lambda_context(clear_state=True)
@metrics.log_metrics
def lambda_handler(event, context):
    try:
        record = build_record(parse_payload(event), request_id=context.aws_request_id)
    except InvalidEvent as exc:
        metrics.add_metric(name="InvalidEvents", unit=MetricUnit.Count, value=1)
        logger.warning("rejected event", reason=str(exc))
        return _response(400, {"error": str(exc)})

    logger.append_keys(event_id=record["event_id"], event_type=record["event_type"])

    started = time.perf_counter()
    try:
        partition, offset = publish(record)
    except Exception:
        metrics.add_metric(name="KafkaPublishFailures", unit=MetricUnit.Count, value=1)
        logger.exception("kafka publish failed")
        return _response(502, {"error": "failed to publish event", "event_id": record["event_id"]})

    metrics.add_metric(
        name="KafkaPublishLatency",
        unit=MetricUnit.Milliseconds,
        value=(time.perf_counter() - started) * 1000,
    )
    metrics.add_metric(name="EventsPublished", unit=MetricUnit.Count, value=1)
    logger.info("published", topic=TOPIC, partition=partition, offset=offset)
    return _response(
        202,
        {
            "status": "accepted",
            "event_id": record["event_id"],
            "topic": TOPIC,
            "partition": partition,
            "offset": offset,
        },
    )
