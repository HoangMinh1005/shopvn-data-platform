"""Direct-order eligibility and customer/refund amounts (BR31 / rule 1.1)."""

from pyspark.sql import functions as F

from shopvn.contracts.direct import MONEY
from shopvn.quality.spark import fail_if


def enrich_orders(orders, returns, customer_ref, earliest_customer):
    refunds = returns.groupBy("order_id").agg(
        F.sum(F.when(F.col("status") == "refunded", F.col("refund_amount")).otherwise(F.lit(0)))
        .cast(MONEY)
        .alias("refunded_order_amount")
    )
    enriched = (
        orders.withColumn("customer_ref", customer_ref("customer_id"))
        .join(earliest_customer, "customer_ref", "left")
        .join(refunds, "order_id", "left")
        .fillna({"refunded_order_amount": 0, "customer_key": 0})
        .withColumn(
            "channel_key",
            F.when(F.col("channel") == "web", 1)
            .when(F.col("channel") == "app", 2)
            .otherwise(0)
            .cast("long"),
        )
        .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("has_excess_discount", F.col("discount_amount") > F.col("subtotal"))
        .withColumn(
            "is_revenue_eligible",
            F.col("status").isin("delivered", "returned")
            & F.col("payment_status").isin("paid", "refunded")
            & (F.col("discount_amount") >= 0)
            & ~F.col("has_excess_discount"),
        )
        .withColumn("merchandise_amount", F.col("subtotal") - F.col("discount_amount"))
        .withColumn("net_customer_amount", F.col("total_amount") - F.col("refunded_order_amount"))
        .withColumn(
            "merchandise_less_order_refund_proxy",
            F.col("merchandise_amount") - F.col("refunded_order_amount"),
        )
    )
    fail_if(enriched, F.col("customer_key") == 0, "Missing customer dimension lookup")

    return enriched


def build_order_fact(enriched):
    order_fact = enriched.select(
        F.sha2(F.concat(F.lit("postgres:"), F.col("order_id")), 256).alias("order_key"),
        F.col("order_id").alias("source_order_id"),
        "customer_key",
        "channel_key",
        "order_date_key",
        "order_date",
        "channel",
        F.col("status").alias("order_status"),
        "payment_status",
        "payment_method",
        "voucher_code",
        F.col("subtotal").alias("subtotal_amount"),
        F.col("discount_amount").alias("header_discount_amount"),
        F.col("shipping_fee").alias("customer_shipping_fee"),
        F.col("total_amount").alias("source_total_amount"),
        "merchandise_amount",
        "refunded_order_amount",
        "net_customer_amount",
        "merchandise_less_order_refund_proxy",
        "has_excess_discount",
        "is_revenue_eligible",
        "source_record_hash",
        "snapshot_id",
    )
    order_fact = order_fact.withColumn(
        "dimension_match_basis", F.lit("initial_snapshot_fallback")
    ).withColumn("currency_code", F.lit("VND"))

    return order_fact
