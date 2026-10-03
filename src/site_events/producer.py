"""Kafka producer setup, shared across warm invocations."""
import json
import os

from confluent_kafka import Producer


def _config():
    conf = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "client.id": "site-events-lambda",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        # Fail a send well inside the function timeout so we can answer 502.
        "message.timeout.ms": int(os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "5000")),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    secret_arn = os.getenv("KAFKA_SECRET_ARN")
    if secret_arn:
        import boto3  # bundled with the Lambda runtime; only needed for SASL

        # Secret JSON: {"username": "...", "password": "..."}
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
        creds = json.loads(secret["SecretString"])
        conf.update(
            {
                "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
                "sasl.username": creds["username"],
                "sasl.password": creds["password"],
            }
        )
    return conf


_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = Producer(_config())
    return _producer
