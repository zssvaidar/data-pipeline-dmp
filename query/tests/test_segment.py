import json
import os
import sys
from datetime import date

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import segment  # noqa: E402

AS_OF = date(2026, 10, 2)
PARAMS = {"window_days": 30, "min_spend": 10000.0}


def write_curated(lake, rows):
    os.makedirs(f"{lake}/curated", exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (event_id VARCHAR, event_type VARCHAR, order_id VARCHAR, "
                "customer_id VARCHAR, amount DOUBLE, event_date DATE)")
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY t TO '{lake}/curated/order_events' (FORMAT parquet, PARTITION_BY (event_date))")


def test_segment_window_threshold_and_dedupe(tmp_path):
    lake = str(tmp_path)
    write_curated(lake, [
        ("e1", "order_placed", "o1", "big", 6000.0, date(2026, 10, 1)),
        ("e2", "order_placed", "o2", "big", 5000.0, date(2026, 9, 20)),
        ("e3", "order_placed", "o3", "dup", 6000.0, date(2026, 10, 1)),
        ("e3", "order_placed", "o3", "dup", 6000.0, date(2026, 10, 1)),  # reprocessed: counted once
        ("e4", "order_placed", "o4", "old", 20000.0, date(2026, 8, 1)),   # outside the 30-day window
        ("e5", "order_placed", "o5", "edge", 10000.0, date(2026, 10, 2)), # not strictly above threshold
    ])
    con = segment.connect(lake)
    customers = segment.build_segment(con, lake, AS_OF, **PARAMS)
    assert customers == [{"customer_id": "big", "total_spend": 11000.0, "order_count": 2}]

    target = segment.activate(con, lake, AS_OF, customers, PARAMS)
    assert target.endswith("activation/high_value_customers/dt=2026-10-02/segment.json")
    with open(target) as f:
        assert [json.loads(line) for line in f] == customers
