"""Order-events ETL: the local stand-in for the bookmarked Glue job.

Reads only the raw behavior-event files not yet processed (a job bookmark),
joins them with the orders table, and appends curated Parquet partitioned
by event_date. The bookmark is committed only after the output is written,
the same contract as Glue's job.commit().

Glue -> local mapping:
  create_dynamic_frame.from_catalog(raw_db.behavior_events)  -> spark.read.json(new files, EVENT_SCHEMA)
  create_dynamic_frame.from_catalog(curated_db.orders)       -> spark.read.jdbc(orders)
  transformation_ctx bookmarks                               -> Bookmark (processed-file list in the lake)
  write_dynamic_frame(glueparquet, partitionKeys)            -> df.write.partitionBy("event_date").parquet
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

log = logging.getLogger("order_events_etl")

JOB_NAME = "order_events_etl"

# Explicit schema in place of the Glue Crawler's inferred one: a schema
# change in the raw data fails loudly here instead of drifting silently.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("ts", StringType()),
])

OUTPUT_COLUMNS = [
    "event_id", "event_type", "order_id", "customer_id", "amount",
    "event_ts", "order_created_at", "order_found", "event_date",
]


def _fs(spark: SparkSession, path: str):
    jvm = spark._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def list_files(spark: SparkSession, root: str) -> list[str]:
    """Recursively list data files under root (empty if root is missing)."""
    fs, jpath = _fs(spark, root)
    if not fs.exists(jpath):
        return []
    files = []
    it = fs.listFiles(jpath, True)
    while it.hasNext():
        status = it.next()
        name = status.getPath().getName()
        if not name.startswith((".", "_")):
            files.append(status.getPath().toString())
    return sorted(files)


class Bookmark:
    """Set of raw files already processed, stored as JSON in the lake."""

    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path
        self.processed: set[str] = set()

    def load(self) -> "Bookmark":
        fs, jpath = _fs(self.spark, self.path)
        if fs.exists(jpath):
            text = self.spark.read.text(self.path, wholetext=True).first()[0]
            self.processed = set(json.loads(text)["processed_files"])
        return self

    def new_files(self, files: list[str]) -> list[str]:
        return [f for f in files if f not in self.processed]

    def commit(self, files: list[str]) -> None:
        self.processed.update(files)
        doc = {
            "job": JOB_NAME,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processed_files": sorted(self.processed),
        }
        fs, jpath = _fs(self.spark, self.path)
        out = fs.create(jpath, True)
        try:
            out.write(bytearray(json.dumps(doc, indent=2).encode("utf-8")))
        finally:
            out.close()


def enrich(events: DataFrame, orders: DataFrame) -> DataFrame:
    """Join events with orders and derive the partition column."""
    # The raw zone is at-least-once, so the same event can land twice.
    events = events.dropDuplicates(["event_id"])
    orders = orders.select("order_id", F.col("created_at").alias("order_created_at"))
    return (
        events.join(F.broadcast(orders), on="order_id", how="left")
        .withColumn("event_ts", F.to_timestamp("ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("order_found", F.col("order_created_at").isNotNull())
        .select(*OUTPUT_COLUMNS)
    )


def run(spark: SparkSession, raw_path: str, curated_path: str, bookmark_path: str, load_orders) -> int:
    """Process new raw files; returns the number of curated rows written."""
    bookmark = Bookmark(spark, bookmark_path).load()
    new_files = bookmark.new_files(list_files(spark, raw_path))
    if not new_files:
        log.info("no new raw files under %s", raw_path)
        return 0
    log.info("processing %d new raw files", len(new_files))

    events = spark.read.schema(EVENT_SCHEMA).json(new_files)
    enriched = enrich(events, load_orders(spark)).cache()
    rows = enriched.count()
    enriched.write.mode("append").partitionBy("event_date").parquet(curated_path)

    # Commit only after the write succeeded: a crash before this line means
    # the files are reprocessed next run (at-least-once), never skipped.
    bookmark.commit(new_files)
    log.info("wrote %d rows to %s", rows, curated_path)
    return rows


def jdbc_orders(spark: SparkSession) -> DataFrame:
    return spark.read.jdbc(
        os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/pipeline"),
        "orders",
        properties={
            "user": os.environ.get("JDBC_USER", "pipeline"),
            "password": os.environ.get("JDBC_PASSWORD", "pipeline"),
            "driver": "org.postgresql.Driver",
        },
    )


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName(JOB_NAME)
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        # Point s3a at MinIO; on AWS Glue none of this is needed.
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    return builder.getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    lake = os.environ.get("LAKE_ROOT", "s3a://data-lake")
    spark = build_spark()
    try:
        run(
            spark,
            raw_path=f"{lake}/raw/behavior-events",
            curated_path=f"{lake}/curated/order_events",
            bookmark_path=f"{lake}/_state/bookmarks/{JOB_NAME}.json",
            load_orders=jdbc_orders,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
