"""Build the high-value customer segment and "activate" it.

Runs segment.sql over the curated zone with DuckDB.
Activation here writes the segment to the lake as newline-delimited JSON and,
if WEBHOOK_URL is set, POSTs it there.
"""

import json
import logging
import os
import pathlib
import urllib.request
from datetime import date, datetime, timezone

import duckdb

log = logging.getLogger("segment")

SQL = (pathlib.Path(__file__).parent / "segment.sql").read_text()
SEGMENT = "high_value_customers"


def connect(lake_root: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if lake_root.startswith("s3://"):
        con.execute("LOAD httpfs")
        endpoint = os.environ.get("S3_ENDPOINT", "")
        if endpoint:
            # MinIO: path-style, endpoint without the scheme.
            con.execute(
                "CREATE SECRET lake (TYPE s3, KEY_ID $key, SECRET $secret, ENDPOINT $endpoint, "
                "URL_STYLE 'path', USE_SSL $ssl, REGION $region)",
                {
                    "key": os.environ["AWS_ACCESS_KEY_ID"],
                    "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
                    "endpoint": endpoint.split("://", 1)[-1],
                    "ssl": endpoint.startswith("https"),
                    "region": os.environ.get("AWS_REGION", "us-east-1"),
                },
            )
        else:
            con.execute("CREATE SECRET lake (TYPE s3, PROVIDER credential_chain)")
    return con


def build_segment(con, lake_root: str, as_of: date, window_days: int, min_spend: float) -> list[dict]:
    con.execute("SET VARIABLE curated_glob = $g", {"g": f"{lake_root}/curated/order_events/*/*.parquet"})
    con.execute("CREATE OR REPLACE TEMP TABLE segment AS " + SQL.rstrip().rstrip(";"),
                {"as_of": as_of, "window_days": window_days, "min_spend": min_spend})
    cur = con.execute("SELECT * FROM segment")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def activate(con, lake_root: str, as_of: date, customers: list[dict], params: dict) -> str:
    """Write the segment (one JSON customer per line) and optionally push it."""
    target = f"{lake_root}/activation/{SEGMENT}/dt={as_of.isoformat()}/segment.json"
    if "://" not in lake_root:
        pathlib.Path(target).parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY segment TO '{target.replace(chr(39), chr(39) * 2)}' (FORMAT json)")

    webhook = os.environ.get("WEBHOOK_URL")
    if webhook:
        body = json.dumps({
            "segment": SEGMENT,
            "as_of": as_of.isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "params": params,
            "customers": customers,
        }, default=str).encode()
        req = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("pushed segment to %s (HTTP %s)", webhook, resp.status)
    return target


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    lake_root = os.environ.get("LAKE_ROOT", "s3://data-lake")
    as_of = date.fromisoformat(os.environ["AS_OF"]) if os.environ.get("AS_OF") else datetime.now(timezone.utc).date()
    params = {
        "window_days": int(os.environ.get("WINDOW_DAYS", "30")),
        "min_spend": float(os.environ.get("MIN_SPEND", "10000")),
    }

    con = connect(lake_root)
    customers = build_segment(con, lake_root, as_of, **params)
    target = activate(con, lake_root, as_of, customers, params)
    log.info("segment %s: %d customers -> %s", SEGMENT, len(customers), target)
    for c in customers:
        log.info("  %s spent %.2f over %d orders", c["customer_id"], c["total_spend"], c["order_count"])


if __name__ == "__main__":
    main()
