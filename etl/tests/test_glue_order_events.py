"""Runs the Glue entry point against stub awsglue modules on local Spark.

The awsglue library only exists on Glue (or its multi-GB container image),
so these stubs stand in for the catalog, DynamoDB and the S3 sink. They
check the script's own logic: type pinning, DynamoDB timestamp parsing,
the empty-batch path, and that the bookmark is committed after the write.
"""

import os
import runpy
import sys
import types
from datetime import date

import pytest

JOBS = os.path.join(os.path.dirname(__file__), "..", "jobs")
sys.path.insert(0, JOBS)

ARGS = {
    "JOB_NAME": "order-events-etl", "RAW_DATABASE": "raw_db", "RAW_TABLE": "behavior_events",
    "ORDERS_TABLE": "Orders", "CURATED_DATABASE": "curated_db", "CURATED_TABLE": "order_events",
    "CURATED_PATH": "s3://lake/curated/order_events/",
}


class Frame:
    def __init__(self, df):
        self.df = df

    def toDF(self):
        return self.df


def install_stubs(monkeypatch, spark, raw_df, orders_df, calls):
    class GlueContext:
        def __init__(self, sc):
            self.spark_session = spark
            self.create_dynamic_frame = types.SimpleNamespace(
                from_catalog=lambda **kw: calls.append(("from_catalog", kw)) or Frame(raw_df),
                from_options=lambda **kw: calls.append(("from_options", kw)) or Frame(orders_df),
            )

        def get_logger(self):
            return types.SimpleNamespace(info=lambda msg: calls.append(("log", msg)))

        def getSink(self, **kw):
            calls.append(("getSink", kw))
            sink = types.SimpleNamespace()
            sink.setFormat = lambda fmt: calls.append(("setFormat", fmt))
            sink.setCatalogInfo = lambda **kw: calls.append(("setCatalogInfo", kw))
            sink.writeFrame = lambda frame: calls.append(("writeFrame", frame.collect()))
            return sink

    class Job:
        def __init__(self, glue):
            pass

        def init(self, name, args):
            calls.append(("init", name))

        def commit(self):
            calls.append(("commit", None))

    modules = {
        "awsglue": types.ModuleType("awsglue"),
        "awsglue.context": types.SimpleNamespace(GlueContext=GlueContext),
        "awsglue.dynamicframe": types.SimpleNamespace(
            DynamicFrame=types.SimpleNamespace(fromDF=lambda df, glue, name: df)),
        "awsglue.job": types.SimpleNamespace(Job=Job),
        "awsglue.utils": types.SimpleNamespace(getResolvedOptions=lambda argv, keys: {k: ARGS[k] for k in keys}),
    }
    for name, mod in modules.items():
        monkeypatch.setitem(sys.modules, name, mod)
    import pyspark.context
    monkeypatch.setattr(pyspark.context, "SparkContext", lambda: spark.sparkContext)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("glue-tests")
         .config("spark.sql.shuffle.partitions", "1").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def orders(spark):
    # DynamoDB hands back the created_at attribute as an ISO string.
    return spark.createDataFrame(
        [("o-1", "c-1", 50.0, "2026-10-01T08:00:00Z")],
        "order_id string, customer_id string, amount double, created_at string")


def test_glue_job_writes_then_commits(spark, monkeypatch):
    # Crawler-inferred types: amount came out as a string, plus partition columns.
    raw = spark.createDataFrame(
        [("e-1", "order_placed", "o-1", "c-1", "50", "2026-10-01T08:00:00Z", "2026", "10", "01")],
        "event_id string, event_type string, order_id string, customer_id string, amount string, "
        "ts string, year string, month string, day string")
    calls = []
    install_stubs(monkeypatch, spark, raw, orders(spark), calls)

    runpy.run_path(os.path.join(JOBS, "glue_order_events.py"), run_name="__main__")

    names = [c[0] for c in calls]
    assert names.index("writeFrame") < names.index("commit"), "bookmark committed before the write"
    assert dict(calls)["from_catalog"]["transformation_ctx"] == "raw_events"
    sink = dict(calls)["getSink"]
    assert sink["partitionKeys"] == ["event_date"] and sink["enableUpdateCatalog"] is True
    assert dict(calls)["setFormat"] == "glueparquet"

    [row] = dict(calls)["writeFrame"]
    assert row.amount == 50.0 and row.order_found is True
    assert row.event_date == date(2026, 10, 1)
    assert "year" not in row.asDict()


def test_glue_job_with_no_new_files_only_commits(spark, monkeypatch):
    from order_events_etl import EVENT_SCHEMA
    calls = []
    install_stubs(monkeypatch, spark, spark.createDataFrame([], EVENT_SCHEMA), orders(spark), calls)

    runpy.run_path(os.path.join(JOBS, "glue_order_events.py"), run_name="__main__")

    names = [c[0] for c in calls]
    assert "writeFrame" not in names and "from_options" not in names
    assert names[-1] == "commit"
