import json
import os
import sys
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
import order_events_etl as etl  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("etl-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def orders_df(spark):
    return spark.createDataFrame(
        [("o-1", "c-1", 50.0, datetime(2026, 10, 1, 8, 0)), ("o-2", "c-2", 75.0, datetime(2026, 10, 2, 9, 0))],
        "order_id string, customer_id string, amount double, created_at timestamp",
    )


def event(event_id, order_id, customer_id, amount, ts):
    return {"event_id": event_id, "event_type": "order_placed", "order_id": order_id,
            "customer_id": customer_id, "amount": amount, "ts": ts}


def write_raw(path, events):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.writelines(json.dumps(e) + "\n" for e in events)


def test_enrich_joins_dedupes_and_partitions(spark):
    events = spark.createDataFrame(
        [
            event("e-1", "o-1", "c-1", 50.0, "2026-10-01T08:00:00Z"),
            event("e-1", "o-1", "c-1", 50.0, "2026-10-01T08:00:00Z"),  # landed twice
            event("e-3", "o-missing", "c-3", 10.0, "2026-10-02T23:59:59Z"),
        ],
        schema=etl.EVENT_SCHEMA,
    )
    rows = {r.event_id: r for r in etl.enrich(events, orders_df(spark)).collect()}

    assert set(rows) == {"e-1", "e-3"}
    assert rows["e-1"].order_found is True
    assert rows["e-1"].order_created_at == datetime(2026, 10, 1, 8, 0)
    assert rows["e-1"].event_date == date(2026, 10, 1)
    assert rows["e-3"].order_found is False
    assert rows["e-3"].event_date == date(2026, 10, 2)
    assert etl.enrich(events, orders_df(spark)).columns == etl.OUTPUT_COLUMNS


def test_run_is_incremental(spark, tmp_path):
    raw = tmp_path / "raw" / "behavior-events"
    curated = str(tmp_path / "curated" / "order_events")
    bookmark = str(tmp_path / "_state" / "bookmark.json")

    def run():
        return etl.run(spark, str(raw), curated, bookmark, load_orders=orders_df)

    write_raw(str(raw / "year=2026/month=10/day=01/a.json"),
              [event("e-1", "o-1", "c-1", 50.0, "2026-10-01T08:00:00Z")])
    assert run() == 1
    assert run() == 0, "already-processed files must be skipped"

    write_raw(str(raw / "year=2026/month=10/day=02/b.json"),
              [event("e-2", "o-2", "c-2", 75.0, "2026-10-02T09:00:00Z")])
    assert run() == 1, "only the new file is processed"

    out = spark.read.parquet(curated)
    assert sorted(r.event_id for r in out.collect()) == ["e-1", "e-2"]
    assert {str(r.event_date) for r in out.select("event_date").distinct().collect()} == {"2026-10-01", "2026-10-02"}
    with open(bookmark) as f:
        assert len(json.load(f)["processed_files"]) == 2


def test_failed_write_does_not_advance_bookmark(spark, tmp_path):
    raw = tmp_path / "raw"
    bookmark = str(tmp_path / "bookmark.json")
    write_raw(str(raw / "a.json"), [event("e-1", "o-1", "c-1", 50.0, "2026-10-01T08:00:00Z")])

    def broken_orders(_spark):
        raise RuntimeError("orders source unavailable")

    with pytest.raises(RuntimeError):
        etl.run(spark, str(raw), str(tmp_path / "out"), bookmark, load_orders=broken_orders)
    assert not os.path.exists(bookmark)

    assert etl.run(spark, str(raw), str(tmp_path / "out"), bookmark, load_orders=orders_df) == 1
