"""Normalize wire encodings and materialize current direct-source snapshots."""

from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table
from shopvn.quality.spark import fail_if


def normalize(name, frame):
    silver_frame = frame
    if name == "products":
        # PostgreSQL COPY encodes booleans t/f; preserve the wire value in Bronze.
        fail_if(
            frame,
            ~F.col("is_active").isin("t", "f", "true", "false"),
            "Unknown PostgreSQL boolean encoding",
        )
        silver_frame = frame.withColumn("is_active", F.col("is_active").isin("t", "true"))
    elif name == "inventory_transactions":
        silver_frame = (
            frame.withColumn("business_time_reliable", F.lit(False))
            .withColumn("business_effective_at", F.lit(None).cast("timestamp"))
            .withColumn("warehouse_id", F.lit(None).cast("string"))
        )
    elif name == "product_reviews":
        silver_frame = frame.filter(F.col("rating").between(1, 5)).withColumn(
            "dq_status", F.lit("valid")
        )

    return silver_frame


def materialize(spark, frames, stage):
    return_tables = {}
    for name, frame in frames.items():
        normalized = normalize(name, frame)
        if name == "product_reviews":
            rejected = frame.filter(
                F.col("rating").isNull() | ~F.col("rating").between(1, 5)
            ).withColumn("quarantine_reason", F.lit("invalid_rating"))
            if normalized.count() + rejected.count() != frame.count():
                raise ValueError("Review accepted/rejected reconciliation failed")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS shopvn.quarantine")
            table = "shopvn.quarantine.direct_product_reviews"
            if not spark.catalog.tableExists(table):
                rejected.writeTo(table).using("iceberg").tableProperty(
                    "format-version", "2"
                ).create()
            elif rejected.limit(1).count():
                new = rejected.join(
                    spark.table(table).select("snapshot_id").distinct(), "snapshot_id", "left_anti"
                )
                new.writeTo(table).append()
        replace_table(spark, normalized, f"shopvn.silver.direct_{name}", stage)
        return_tables[name] = spark.table(f"shopvn.silver.direct_{name}")
    return return_tables
