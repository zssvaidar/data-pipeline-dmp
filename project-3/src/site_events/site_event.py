"""Pure event logic: parsing and validation. No AWS or Kafka dependencies."""
import base64
import json
import time
import uuid

ALLOWED_EVENT_TYPES = frozenset(
    {
        "page_view",
        "click",
        "search",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "purchase",
        "signup",
        "login",
        "logout",
        "error",
    }
)


class InvalidEvent(ValueError):
    pass


def parse_payload(event):
    """Return the site event from an API Gateway (v1/v2) proxy event or a direct invoke."""
    if isinstance(event, dict) and "body" in event:
        body = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode()
        try:
            payload = json.loads(body) if isinstance(body, str) else body
        except json.JSONDecodeError as exc:
            raise InvalidEvent("body is not valid JSON") from exc
    else:
        payload = event

    if not isinstance(payload, dict):
        raise InvalidEvent("event must be a JSON object")
    return payload


def build_record(payload, request_id=None, now_ms=None):
    """Validate a site event and normalise it into the record written to Kafka."""
    event_type = payload.get("event_type")
    if event_type not in ALLOWED_EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type {event_type!r}")

    properties = payload.get("properties", {})
    if not isinstance(properties, dict):
        raise InvalidEvent("properties must be an object")

    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    return {
        "event_id": payload.get("event_id") or str(uuid.uuid4()),
        "event_type": event_type,
        "ts": payload.get("ts") or now_ms,
        "user_id": payload.get("user_id"),
        "session_id": payload.get("session_id"),
        "properties": properties,
        "ingested_at": now_ms,
        "request_id": request_id,
    }


def partition_key(record):
    # Keep a session's events ordered on one partition.
    return str(record["session_id"] or record["user_id"] or record["event_id"])
