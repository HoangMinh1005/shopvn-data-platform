"""Verified CSV → immutable Bronze versions → typed, source-preserving active rows."""

import hashlib

from pyspark.sql import functions as F

from shopvn.common.iceberg import with_hash
from shopvn.contracts.marketplace import COLUMNS, SCHEMA
from shopvn.quality.spark import fail_if


def read_active(spark, active, paths, *, bronze_table="shopvn.bronze.marketplace_sales_line"):
    versions, new_frames, references = [], [], {}
    known = set()
    if spark.catalog.tableExists(bronze_table):
        known = {
            r[0] for r in spark.table(bronze_table).select("file_version_id").distinct().collect()
        }
    schema = spark.createDataFrame([], SCHEMA).schema
    for item in active:
        path = paths.runtime / "marketplace/landing" / item["capture_id"] / item["filename"]
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != item["sha256"]:
                raise ValueError("Verified marketplace artifact was changed")
        version = hashlib.sha256(
            f"{item['partner']}:{item['business_date']}:{item['sha256']}".encode()
        ).hexdigest()
        if version not in known:
            raw = spark.read.option("header", True).option("mode", "FAILFAST").csv(str(path))
            raw = with_hash(raw.select(*COLUMNS), "source_record_hash", COLUMNS)
            raw = (
                raw.withColumn("file_version_id", F.lit(version))
                .withColumn("source_file_date", F.lit(item["business_date"]).cast("date"))
                .withColumn("source_file_partner", F.lit(item["partner"]))
                .withColumn("snapshot_id", F.lit(item["capture_id"]))
                .withColumn("observed_at", F.to_timestamp(F.lit(item["observed_at"])))
            )
            new_frames.append(raw)
            known.add(version)
        versions.append(version)
        references[version] = item["reference"]["accepted"]
    if new_frames:
        incoming = new_frames[0]
        for frame in new_frames[1:]:
            incoming = incoming.unionByName(frame)
        if spark.catalog.tableExists(bronze_table):
            incoming.coalesce(2).writeTo(bronze_table).append()
        else:
            incoming.coalesce(2).writeTo(bronze_table).using("iceberg").tableProperty(
                "format-version", "2"
            ).create()
    if not versions:
        result = spark.createDataFrame(
            [],
            SCHEMA
            + ",source_record_hash string,file_version_id string,snapshot_id string,observed_at timestamp",
        )
    else:
        raw = spark.table(bronze_table).filter(F.col("file_version_id").isin(versions))
        result = raw.select(
            *[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields],
            "source_record_hash",
            "file_version_id",
            "snapshot_id",
            "observed_at",
        ).dropDuplicates(list(COLUMNS))
        counts = {r[0]: r[1] for r in result.groupBy("file_version_id").count().collect()}
        if any(counts.get(k, 0) != v for k, v in references.items()):
            raise ValueError("Marketplace accepted source count mismatch")
    keys = ["partner", "external_order_id", "shopvn_product_id"]
    if result.select(keys).distinct().count() != result.count():
        raise ValueError("Marketplace duplicate identity across active files")
    result = result.withColumn(
        "formula_delta_vnd",
        (
            F.col("net_revenue")
            - F.floor(F.col("sale_price") * F.col("quantity_sold") * (1 - F.col("commission_rate")))
        ).cast("decimal(20,2)"),
    )
    fail_if(
        result,
        F.col("formula_delta_vnd").isNull() | ~F.col("formula_delta_vnd").isin(0, -1),
        "Unexplained marketplace formula delta",
    )
    return result.withColumn("is_sales_eligible", F.col("status") == "completed")
