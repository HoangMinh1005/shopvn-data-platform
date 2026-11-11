"""Order-grain return records; do not invent item-level refund allocation."""

from pyspark.sql import functions as F

from shopvn.contracts.direct import MONEY


def build_return_fact(returns, enriched):
    return_fact = returns.join(
        enriched.select("order_id", "customer_key", "channel_key", "order_date_key"), "order_id"
    ).select(
        F.sha2(F.concat(F.lit("postgres:return:"), F.col("return_id")), 256).alias("return_key"),
        F.col("return_id").cast("long").alias("source_return_id"),
        F.col("order_id").alias("source_order_id"),
        "customer_key",
        "channel_key",
        "order_date_key",
        F.col("status").alias("return_status"),
        F.col("reason").alias("return_reason"),
        F.col("refund_amount").alias("source_refund_amount"),
        F.when(F.col("status") == "refunded", F.col("refund_amount"))
        .otherwise(0)
        .cast(MONEY)
        .alias("refunded_order_amount"),
        F.col("created_at").alias("source_request_created_at"),
        "source_record_hash",
        "snapshot_id",
    )

    return return_fact
