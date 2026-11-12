"""Stable line-level discount allocation, preserving the fractional VND tail."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopvn.contracts.direct import MONEY
from shopvn.quality.spark import fail_if


def build_sales_line_fact(lines, enriched, earliest_product):
    if not {"product_id", "order_date", "product_key", "product_dimension_match_basis"} <= set(
        earliest_product.columns
    ):
        raise ValueError("Product lookup requires business date and match basis")
    lookup_keys = ["product_id", "order_date"]
    line_base = lines.join(
        enriched.select(
            "order_id",
            "customer_key",
            "channel_key",
            "order_date_key",
            "order_date",
            "customer_dimension_match_basis",
            "subtotal",
            "discount_amount",
            "is_revenue_eligible",
            "has_excess_discount",
        ),
        "order_id",
    ).join(earliest_product, lookup_keys, "left")
    fail_if(line_base, F.col("product_key").isNull(), "Missing product dimension lookup")
    positive = line_base.filter((F.col("quantity") > 0) & ~F.col("has_excess_discount"))
    partition = Window.partitionBy("order_id")
    positive = (
        positive.withColumn("basis", F.sum("total_price").over(partition))
        .withColumn(
            "raw_allocation",
            F.when(
                F.col("basis") > 0,
                F.col("discount_amount") * F.col("total_price") / F.col("basis"),
            ).otherwise(0),
        )
        .withColumn("floor_allocation", F.floor("raw_allocation").cast(MONEY))
        .withColumn(
            "remainder", F.col("discount_amount") - F.sum("floor_allocation").over(partition)
        )
        .withColumn(
            "rank",
            F.row_number().over(
                partition.orderBy(
                    (F.col("raw_allocation") - F.col("floor_allocation")).desc(),
                    F.col("item_id"),
                )
            ),
        )
    )
    allocations = positive.select(
        "item_id",
        (
            F.col("floor_allocation")
            + F.when(F.col("rank") <= F.floor("remainder"), 1).otherwise(0)
            + F.when(F.col("rank") == 1, F.col("remainder") - F.floor("remainder")).otherwise(0)
        )
        .cast(MONEY)
        .alias("allocated_header_discount_amount"),
    )
    line_base = line_base.join(allocations, "item_id", "left").withColumn(
        "allocated_header_discount_amount",
        F.when(F.col("quantity") == 0, F.lit(0).cast(MONEY)).otherwise(
            F.col("allocated_header_discount_amount")
        ),
    )
    line_fact = line_base.select(
        F.sha2(F.concat(F.lit("postgres:item:"), F.col("item_id")), 256).alias("sales_line_key"),
        F.col("item_id").cast("string").alias("source_line_id"),
        F.col("order_id").alias("source_order_id"),
        "product_key",
        "customer_key",
        "channel_key",
        "order_date_key",
        "customer_dimension_match_basis",
        "product_dimension_match_basis",
        "quantity",
        (F.col("unit_price") - F.col("discount_per_item")).alias("unit_sale_price"),
        (F.col("discount_per_item") * F.col("quantity")).cast(MONEY).alias("item_discount_amount"),
        F.col("total_price").alias("line_merchandise_before_header_discount"),
        "allocated_header_discount_amount",
        (F.col("total_price") - F.col("allocated_header_discount_amount"))
        .cast(MONEY)
        .alias("net_merchandise_before_refund"),
        (F.col("is_revenue_eligible") & (F.col("quantity") > 0)).alias("is_sales_eligible"),
        "source_record_hash",
        "snapshot_id",
    )

    return line_fact
