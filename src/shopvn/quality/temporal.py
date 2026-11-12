"""SCD2 interval and observation-order gates."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopvn.common.time import utc_instant
from shopvn.quality.spark import fail_if


def validate_history(df, business, key):
    fail_if(
        df,
        F.col(key).isNull()
        | F.col(business).isNull()
        | F.col("valid_from").isNull()
        | F.col("is_current").isNull(),
        "Null dimension identity/validity",
    )
    if df.select(key).distinct().count() != df.count():
        raise ValueError("Duplicate dimension surrogate key")
    fail_if(
        df,
        (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")))
        | (F.col("is_current") != F.col("valid_to").isNull()),
        "Invalid dimension interval/current flag",
    )
    current = df.groupBy(business).agg(
        F.sum(F.col("is_current").cast("int")).alias("current_count")
    )
    fail_if(current, F.col("current_count") != 1, "Dimension must have exactly one current version")
    ordered = df.withColumn(
        "next_from", F.lead("valid_from").over(Window.partitionBy(business).orderBy("valid_from"))
    )
    fail_if(
        ordered,
        F.col("next_from").isNotNull()
        & (F.col("valid_to").isNull() | (F.col("valid_to") > F.col("next_from"))),
        "Overlapping dimension intervals",
    )


def assert_observation_order(
    spark, observed_at, tables=("shopvn.gold.dim_customer", "shopvn.gold.dim_product")
):
    instant = utc_instant(observed_at).isoformat()
    for table in tables:
        if spark.catalog.tableExists(table):
            prior = spark.table(table)
            clock = "last_observed_at" if "last_observed_at" in prior.columns else "valid_from"
            fail_if(
                prior,
                F.col(clock) > F.to_timestamp(F.lit(instant)),
                "Stale observation: replay must not rewrite newer observed history",
            )
