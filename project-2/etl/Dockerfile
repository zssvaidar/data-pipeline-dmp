# Spark runtime for the ETL job.
FROM apache/spark:3.5.3-python3

# s3a for MinIO/S3 (versions must match Spark's bundled Hadoop 3.3.4) and
# the Postgres JDBC driver for the orders table.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

COPY jobs/ /opt/etl/jobs/
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--conf", "spark.sql.session.timeZone=UTC", "/opt/etl/jobs/order_events_etl.py"]
