"""Observed customer/product versions and source-date-grain lookups."""

import hashlib
import hmac

from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table
from shopvn.transformations.gold.temporal import lookup_day_versions, observed_dimension


def ensure_dates(spark, dates, stage):
    incoming = dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date"
    )
    if spark.catalog.tableExists("shopvn.gold.dim_date"):
        incoming = incoming.unionByName(
            spark.table("shopvn.gold.dim_date").select("date_key", "full_date")
        )
    incoming = incoming.union(
        spark.createDataFrame([(0, None)], "date_key int,full_date date")
    ).dropDuplicates(["date_key"])
    replace_table(spark, incoming, "shopvn.gold.dim_date", stage)


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

    customer_requests = orders.select(
        customer_ref("customer_id").alias("customer_ref"), "order_date"
    )
    product_requests = (
        frames["order_items"]
        .join(orders.select("order_id", "order_date"), "order_id")
        .select("product_id", "order_date")
    )
    customers = lookup_day_versions(
        customer_requests, dim_customer, "customer_ref", "customer_key", observed_at
    ).withColumnRenamed("dimension_match_basis", "customer_dimension_match_basis")
    products = lookup_day_versions(
        product_requests, dim_product, "product_id", "product_key", observed_at
    ).withColumnRenamed("dimension_match_basis", "product_dimension_match_basis")
    channels = spark.createDataFrame(
        [(0, "unknown", "unknown"), (1, "direct_web", "direct"), (2, "direct_app", "direct")],
        "channel_key long,channel_code string,channel_group string",
    )
    replace_table(spark, channels, "shopvn.gold.dim_channel", stage)
    ensure_dates(spark, orders.select(F.col("order_date").alias("full_date")), stage)
    return customer_ref, customers, products
