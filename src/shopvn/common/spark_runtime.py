"""Spark runtime for the isolated local lakehouse, shared by spike and direct slice."""

import os

from pyspark.sql import SparkSession


def session(app_name: str):
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )
    values = {
        "": "org.apache.iceberg.spark.SparkCatalog",
        ".type": "jdbc",
        ".uri": "jdbc:postgresql://catalog:5432/iceberg_catalog",
        ".jdbc.user": "iceberg",
        ".jdbc.password": os.environ["SPIKE_DB_PASSWORD"],
        ".jdbc.schema-version": "V1",
        ".warehouse": "s3://shopvn-readiness/warehouse",
        ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        ".s3.endpoint": "http://minio:9000",
        ".s3.path-style-access": "true",
        ".s3.access-key-id": os.environ["SPIKE_MINIO_USER"],
        ".s3.secret-access-key": os.environ["SPIKE_MINIO_PASSWORD"],
        ".client.region": "us-east-1",
    }
    for suffix, value in values.items():
        builder = builder.config("spark.sql.catalog.shopvn" + suffix, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def acquire_writer_lock(spark):
    """One transaction-independent advisory lock shared by all direct-slice writers."""
    properties = spark._jvm.java.util.Properties()
    properties.setProperty("user", "iceberg")
    properties.setProperty("password", os.environ["SPIKE_DB_PASSWORD"])
    connection = spark._jvm.org.postgresql.Driver().connect(
        "jdbc:postgresql://catalog:5432/iceberg_catalog", properties
    )
    result = connection.createStatement().executeQuery("SELECT pg_try_advisory_lock(732660001)")
    result.next()
    if not result.getBoolean(1):
        connection.close()
        raise RuntimeError("Another direct-slice writer holds the advisory lock")
    return connection  # Closing the connection releases the lock, including on process failure.
