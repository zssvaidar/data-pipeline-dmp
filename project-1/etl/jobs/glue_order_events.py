"""AWS Glue entry point for the order-events ETL (Glue 5.0, Spark 3.5).

Same transform as the local job (order_events_etl.enrich, shipped with
--extra-py-files), but with Glue's native pieces:
  - raw events come from the crawler's catalog table with a job bookmark
    (transformation_ctx), so each run reads only new files;
  - orders are read straight from DynamoDB;
  - output is written as glueparquet and the curated table's partitions are
    added to the Data Catalog by the sink, so Athena sees them immediately.
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from order_events_etl import EVENT_SCHEMA, enrich

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "RAW_DATABASE", "RAW_TABLE", "ORDERS_TABLE",
    "CURATED_DATABASE", "CURATED_TABLE", "CURATED_PATH",
])

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)
log = glue.get_logger()

raw = glue.create_dynamic_frame.from_catalog(
    database=args["RAW_DATABASE"],
    table_name=args["RAW_TABLE"],
    transformation_ctx="raw_events",  # bookmark: only files added since the last commit
)
events = raw.toDF()

if events.rdd.isEmpty():
    log.info("no new raw files")
else:
    # The crawler infers types; pin them to the contract so drift fails loudly.
    events = events.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in EVENT_SCHEMA.fields])

    orders = glue.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": args["ORDERS_TABLE"],
            # Leave capacity for the API. At larger scale use a DynamoDB
            # export to S3 ("dynamodb.export": "ddb") instead of a scan.
            "dynamodb.throughput.read.percent": "0.5",
        },
    ).toDF().withColumn("created_at", F.to_timestamp("created_at"))

    enriched = enrich(events, orders).repartition("event_date").cache()
    rows = enriched.count()

    sink = glue.getSink(
        connection_type="s3",
        path=args["CURATED_PATH"],
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["event_date"],
        transformation_ctx="curated_sink",
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=args["CURATED_DATABASE"], catalogTableName=args["CURATED_TABLE"])
    sink.writeFrame(DynamicFrame.fromDF(enriched, glue, "enriched"))
    log.info(f"wrote {rows} rows to {args['CURATED_PATH']}")

job.commit()  # advance the bookmark only after the write succeeded
