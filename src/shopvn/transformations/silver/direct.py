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

    return silver_frame


def materialize(spark, frames, stage):
    return_tables = {}
    for name, frame in frames.items():
        replace_table(spark, normalize(name, frame), f"shopvn.silver.direct_{name}", stage)
        return_tables[name] = spark.table(f"shopvn.silver.direct_{name}")
    return return_tables
