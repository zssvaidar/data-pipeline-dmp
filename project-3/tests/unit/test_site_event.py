import base64
import json

import pytest

from site_event import InvalidEvent, build_record, parse_payload, partition_key


def apigw(body, b64=False):
    raw = json.dumps(body)
    if b64:
        raw = base64.b64encode(raw.encode()).decode()
    return {"version": "2.0", "body": raw, "isBase64Encoded": b64}


def test_parses_api_gateway_body():
    assert parse_payload(apigw({"event_type": "click"})) == {"event_type": "click"}


def test_parses_base64_body():
    assert parse_payload(apigw({"event_type": "click"}, b64=True)) == {"event_type": "click"}


def test_direct_invoke_passes_through():
    assert parse_payload({"event_type": "login"}) == {"event_type": "login"}


@pytest.mark.parametrize("event", [{"body": "not json"}, {"body": "[1, 2]"}, ["x"]])
def test_rejects_malformed(event):
    with pytest.raises(InvalidEvent):
        parse_payload(event)


def test_rejects_unknown_event_type():
    with pytest.raises(InvalidEvent, match="unknown event_type"):
        build_record({"event_type": "bogus"})


def test_rejects_non_object_properties():
    with pytest.raises(InvalidEvent):
        build_record({"event_type": "click", "properties": "x"})


def test_build_record_fills_defaults():
    record = build_record({"event_type": "purchase", "user_id": "u1"}, request_id="r1", now_ms=42)
    assert record["event_type"] == "purchase"
    assert record["ts"] == 42 and record["ingested_at"] == 42
    assert record["request_id"] == "r1"
    assert record["event_id"]
    assert record["properties"] == {}


def test_partition_key_prefers_session():
    assert partition_key({"session_id": "s", "user_id": "u", "event_id": "e"}) == "s"
    assert partition_key({"session_id": None, "user_id": "u", "event_id": "e"}) == "u"
    assert partition_key({"session_id": None, "user_id": None, "event_id": "e"}) == "e"
