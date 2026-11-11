"""FIN02 daily/channel aggregation; source-order-cohort restatement semantics."""

from pyspark.sql import functions as F


def build_fin02(orders):
    mart = (
        orders.filter("is_revenue_eligible")
        .groupBy("order_date", "channel")
        .agg(
            F.count("*").alias("eligible_order_count"),
            F.sum("source_total_amount").alias("source_customer_amount"),
            F.sum("refunded_order_amount").alias("refunded_order_amount"),
            F.sum("net_customer_amount").alias("net_customer_amount"),
            F.sum("merchandise_less_order_refund_proxy").alias(
                "merchandise_less_order_refund_proxy"
            ),
        )
    )

    return mart
