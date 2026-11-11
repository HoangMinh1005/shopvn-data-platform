"""Isolated synthetic Iceberg write/MERGE/schema-evolution probe."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("shopvn-readiness").config(
    "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
).config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC")
catalog = {
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
for suffix, value in catalog.items():
    builder = builder.config("spark.sql.catalog.shopvn" + suffix, value)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
try:
    jvm = spark._jvm
    credentials = jvm.software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
        os.environ["SPIKE_MINIO_USER"], os.environ["SPIKE_MINIO_PASSWORD"]
    )
    s3 = jvm.software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(
        jvm.java.net.URI.create("http://minio:9000")
    ).region(jvm.software.amazon.awssdk.regions.Region.US_EAST_1).credentialsProvider(
        jvm.software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(credentials)
    ).forcePathStyle(True).build()
    buckets = s3.listBuckets().buckets()
    names = [buckets.get(i).name() for i in range(buckets.size())]
    if "shopvn-readiness" not in names:
        request = jvm.software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder().bucket(
            "shopvn-readiness"
        ).build()
        s3.createBucket(request)
    s3.close()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS shopvn.readiness")
    spark.sql("""CREATE TABLE IF NOT EXISTS shopvn.readiness.sales_probe
              (line_id STRING, amount DECIMAL(18,2)) USING iceberg
              TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')""")
    spark.sql("""MERGE INTO shopvn.readiness.sales_probe t
              USING (SELECT 'A' line_id, CAST(100 AS DECIMAL(18,2)) amount) s
              ON t.line_id=s.line_id WHEN NOT MATCHED THEN INSERT (line_id,amount) VALUES (s.line_id,s.amount)""")
    if "source_system" not in spark.table("shopvn.readiness.sales_probe").columns:
        spark.sql("ALTER TABLE shopvn.readiness.sales_probe ADD COLUMNS (source_system STRING)")
    spark.sql("""CREATE OR REPLACE TEMP VIEW desired AS
              SELECT 'A' line_id, CAST(125 AS DECIMAL(18,2)) amount, 'simulated' source_system
              UNION ALL SELECT 'B', CAST(200 AS DECIMAL(18,2)), 'simulated'""")
    spark.sql("""MERGE INTO shopvn.readiness.sales_probe t USING desired s ON t.line_id=s.line_id
              WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    metrics = spark.sql("""SELECT COUNT(*) row_count, SUM(amount) total_amount,
                       COUNT(DISTINCT line_id) distinct_keys FROM shopvn.readiness.sales_probe""").first()
    assert metrics.row_count == 2 and str(metrics.total_amount) == "325.00" and metrics.distinct_keys == 2
    jars = {}
    for pattern in ("iceberg-*.jar", "postgresql-*.jar"):
        for jar in Path("/opt/spark/jars").glob(pattern):
            with jar.open("rb") as stream:
                jars[jar.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    report = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "java_version": jvm.java.lang.System.getProperty("java.version"),
        "scala_version": jvm.scala.util.Properties.versionNumberString(),
        "row_count": metrics.row_count, "total_amount": str(metrics.total_amount),
        "schema_addition": "source_system" in spark.table("shopvn.readiness.sales_probe").columns,
        "jar_sha256": jars,
    }
    output = Path("/workspace/runtime/spike")
    output.mkdir(parents=True, exist_ok=True)
    (output / "spark-result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("SPIKE_RESULT=" + json.dumps(report), flush=True)
finally:
    spark.stop()
