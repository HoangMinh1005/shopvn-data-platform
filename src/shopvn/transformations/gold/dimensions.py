"""Observed dimension versions and explicitly limited pre-go-live lookups."""

import hashlib
import hmac
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table, with_hash


def add_unknown(df, key: str, business: str, business_value):
    defaults = {
        key: 0,
        business: business_value,
        "is_current": True,
        "history_reliable": False,
        "attribute_hash": "unknown",
        "valid_from": datetime(1970, 1, 1, tzinfo=timezone.utc),
    }
    row = df.sparkSession.range(1).select(
        *[
            F.lit(defaults.get(field.name)).cast(field.dataType).alias(field.name)
            for field in df.schema.fields
        ]
    )
    return df.unionByName(row)


def observed_dimension(spark, incoming, table, key, business, observed_at, stage):
    """Persist SK/version mapping in the dimension; advisory lock serializes allocation."""
    incoming = with_hash(incoming, "attribute_hash")
    prior = spark.table(table) if spark.catalog.tableExists(table) else None
    if prior is None:
        maximum = 0
        changed = incoming
    else:
        maximum = prior.agg(F.max(key)).first()[0] or 0
        current = prior.filter(F.col("is_current") & (F.col(key) != 0)).select(
            business, F.col("attribute_hash").alias("previous_hash")
        )
        changed = (
            incoming.join(current, business, "left")
            .filter(
                F.col("previous_hash").isNull()
                | (F.col("attribute_hash") != F.col("previous_hash"))
            )
            .drop("previous_hash")
        )
    if prior is not None and changed.limit(1).count() == 0:
        return prior
    new = (
        changed.withColumn(
            key, (F.row_number().over(Window.orderBy(business)) + maximum).cast("long")
        )
        .withColumn("valid_from", F.to_timestamp(F.lit(observed_at)))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("history_reliable", F.lit(False))
    )
    if prior is None:
        result = add_unknown(new, key, business, "0" if business == "customer_ref" else 0)
    else:
        marker = changed.select(business).withColumn("changed", F.lit(True))
        history = (
            prior.join(marker, business, "left")
            .withColumn(
                "valid_to",
                F.when(
                    F.col("changed") & F.col("is_current"), F.to_timestamp(F.lit(observed_at))
                ).otherwise(F.col("valid_to")),
            )
            .withColumn(
                "is_current",
                F.when(F.col("changed") & F.col("is_current"), F.lit(False)).otherwise(
                    F.col("is_current")
                ),
            )
            .drop("changed")
        )
        result = history.unionByName(new)
    replace_table(spark, result, table, stage)
    return spark.table(table)


def build_dimensions(spark, frames, observed_at, stage, hmac_key):
    orders = frames["orders"]
    raw = frames
    secret = hmac_key.read_bytes()
    customer_ref = F.udf(
        lambda value: hmac.new(secret, f"postgres:{value}".encode(), hashlib.sha256).hexdigest(),
        "string",
    )
    customers = raw["customers"].select(
        customer_ref("customer_id").alias("customer_ref"),
        "loyalty_tier",
        "gender",
        F.col("city").alias("customer_city"),
    )
    products = (
        raw["products"]
        .drop("source_record_hash", "snapshot_id")
        .withColumnRenamed("name", "product_name")
    )
    dim_customer = observed_dimension(
        spark,
        customers,
        "shopvn.gold.dim_customer",
        "customer_key",
        "customer_ref",
        observed_at,
        stage,
    )
    dim_product = observed_dimension(
        spark,
        products,
        "shopvn.gold.dim_product",
        "product_key",
        "product_id",
        observed_at,
        stage,
    )
    # Use the first observed version for pre-go-live June facts; never claim historic tier/cost.
    earliest_customer = (
        dim_customer.withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("customer_ref").orderBy("valid_from")),
        )
        .filter("rank=1")
        .select("customer_ref", "customer_key")
    )
    earliest_product = (
        dim_product.withColumn(
            "rank", F.row_number().over(Window.partitionBy("product_id").orderBy("valid_from"))
        )
        .filter("rank=1")
        .select("product_id", "product_key")
    )
    latest_business_date = orders.agg(F.max("order_date")).first()[0]
    if str(latest_business_date) >= observed_at[:10]:
        raise ValueError(
            "This first slice only supports pre-go-live cohort lookup; implement date-grain as-of policy before extending"
        )
    channels = spark.createDataFrame(
        [(0, "unknown", "unknown"), (1, "direct_web", "direct"), (2, "direct_app", "direct")],
        "channel_key long, channel_code string, channel_group string",
    )
    replace_table(spark, channels, "shopvn.gold.dim_channel", stage)
    dates = (
        orders.select("order_date")
        .distinct()
        .select(
            F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("order_date").alias("full_date"),
        )
    )
    replace_table(
        spark,
        dates.union(spark.createDataFrame([(0, None)], "date_key int,full_date date")),
        "shopvn.gold.dim_date",
        stage,
    )

    return customer_ref, earliest_customer, earliest_product
