import json
from types import SimpleNamespace

import pytest

import app


class FakeMessage:
    def partition(self):
        return 1

    def offset(self):
        return 7


class FakeProducer:
    def __init__(self, error=None):
        self.error = error
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, json.loads(value)))
        self._cb = on_delivery

    def flush(self, timeout):
        self._cb(self.error, FakeMessage())


@pytest.fixture
def context():
    return SimpleNamespace(
        function_name="site-events-test",
        memory_limit_in_mb=256,
        invoked_function_arn="arn:aws:lambda:us-east-1:123456789012:function:site-events-test",
        aws_request_id="req-1",
    )


@pytest.fixture
def producer(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(app, "get_producer", lambda: fake)
    return fake


def call(body, context):
    resp = app.lambda_handler({"body": json.dumps(body)}, context)
    return resp["statusCode"], json.loads(resp["body"])


def test_publishes_valid_event(producer, context):
    status, body = call({"event_type": "page_view", "session_id": "s1"}, context)
    assert status == 202
    assert body["partition"] == 1 and body["offset"] == 7
    topic, key, value = producer.sent[0]
    assert topic == app.TOPIC and key == b"s1"
    assert value["request_id"] == "req-1"


def test_invalid_event_is_400_and_not_sent(producer, context):
    status, body = call({"event_type": "nope"}, context)
    assert status == 400
    assert producer.sent == []


def test_kafka_failure_is_502(producer, context):
    producer.error = "broker down"
    status, body = call({"event_type": "click"}, context)
    assert status == 502
    assert body["event_id"]
