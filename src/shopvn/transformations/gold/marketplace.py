"""Conformed marketplace lines, no external-order join to PostgreSQL orders."""

from pyspark.sql import functions as F

from shopvn.contracts.direct import MONEY
from shopvn.quality.spark import fail_if
from shopvn.transformations.gold.dimensions import ensure_channels, ensure_dates
from shopvn.transformations.gold.temporal import lookup_day_versions


def build_lines(
    spark,
    silver,
    observed_at,
    stage,
    *,
    product_table="shopvn.gold.dim_product",
    channel_table="shopvn.gold.dim_channel",
    date_table="shopvn.gold.dim_date",
):
    channels = ensure_channels(spark, stage, channel_table)
    requests = silver.select(F.col("shopvn_product_id").alias("product_id"), "order_date")
    lookup = lookup_day_versions(
        requests, spark.table(product_table), "product_id", "product_key", observed_at
    )
    base = (
        silver.withColumnRenamed("shopvn_product_id", "product_id")
        .join(lookup, ["product_id", "order_date"], "left")
        .join(channels.select(F.col("channel_code").alias("partner"), "channel_key"), "partner")
    )
    fail_if(base, F.col("product_key").isNull(), "Unknown marketplace product")
    ensure_dates(
        spark,
        silver.select(F.col("order_date").alias("full_date")).union(
            silver.select(F.col("settlement_date").alias("full_date"))
        ),
        stage,
        date_table,
    )
    base = base.withColumn("source_system", F.concat(F.lit("marketplace/"), "partner")).withColumn(
        "source_line_id", F.to_json(F.struct("partner", "external_order_id", "product_id"))
    )
    return base.select(
        F.sha2(F.concat_ws(":", "source_system", "source_line_id"), 256).alias("sales_line_key"),
        "source_system",
        F.lit("source").alias("data_origin"),
        "source_line_id",
        F.col("external_order_id").alias("source_order_id"),
        "product_key",
        F.lit(0).cast("long").alias("customer_key"),
        "channel_key",
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("order_date_key"),
        F.date_format("settlement_date", "yyyyMMdd")
        .cast("int")
        .alias("scheduled_settlement_date_key"),
        F.lit("unknown_no_source").alias("customer_dimension_match_basis"),
        F.col("dimension_match_basis").alias("product_dimension_match_basis"),
        F.col("quantity_sold").alias("quantity"),
        F.col("sale_price").alias("unit_sale_price"),
        F.lit(None).cast(MONEY).alias("item_discount_amount"),
        (F.col("sale_price") * F.col("quantity_sold"))
        .cast(MONEY)
        .alias("line_merchandise_before_header_discount"),
        F.lit(0).cast(MONEY).alias("allocated_header_discount_amount"),
        (F.col("sale_price") * F.col("quantity_sold"))
        .cast(MONEY)
        .alias("net_merchandise_before_refund"),
        F.col("net_revenue").alias("partner_reported_net_amount"),
        ((F.col("sale_price") * F.col("quantity_sold")) - F.col("net_revenue"))
        .cast(MONEY)
        .alias("implied_partner_deduction"),
        "commission_rate",
        F.col("platform_discount").alias("source_platform_discount"),
        "formula_delta_vnd",
        F.col("status").alias("line_status"),
        "is_sales_eligible",
        "source_record_hash",
        "snapshot_id",
        "observed_at",
        "file_version_id",
        F.current_timestamp().alias("loaded_at"),
    )
