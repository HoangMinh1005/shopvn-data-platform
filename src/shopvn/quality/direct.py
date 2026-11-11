"""Cross-table/source reconciliation gates for the direct Finance pipeline."""

from pyspark.sql import functions as F

from shopvn.quality.spark import fail_if


def validate_silver(frames):
    orders, lines = frames["orders"], frames["order_items"]
    fail_if(lines, (F.col("quantity") < 0) | F.col("quantity").isNull(), "Invalid quantity")
    fail_if(
        lines,
        F.col("total_price")
        != (F.col("unit_price") - F.col("discount_per_item")) * F.col("quantity"),
        "Line price mismatch",
    )
    line_totals = lines.groupBy("order_id").agg(F.sum("total_price").alias("line_total"))
    fail_if(
        orders.join(line_totals, "order_id", "left"),
        F.col("line_total").isNull() | (F.col("subtotal") != F.col("line_total")),
        "Subtotal reconciliation mismatch",
    )
    fail_if(
        orders,
        F.col("total_amount")
        != F.greatest(
            F.lit(0), F.col("subtotal") + F.col("shipping_fee") - F.col("discount_amount")
        ),
        "Source total mismatch",
    )


def validate_allocated_lines(order_fact, line_fact):
    line_recon = line_fact.groupBy("source_order_id").agg(
        F.sum("net_merchandise_before_refund").alias("line_merchandise")
    )
    fail_if(
        order_fact.filter("is_revenue_eligible").join(line_recon, "source_order_id"),
        F.col("merchandise_amount") != F.col("line_merchandise"),
        "Allocated line-to-header mismatch",
    )


def validate_gold_counts(spark, manifest):
    fact_counts = {}
    for table_name, source_name, primary_key in (
        ("fact_order", "orders", "order_key"),
        ("fact_sales_line", "order_items", "sales_line_key"),
        ("fact_return", "returns", "return_key"),
    ):
        persisted = spark.table(f"shopvn.gold.{table_name}")
        fact_counts[table_name] = persisted.count()
        if fact_counts[table_name] != manifest["tables"][source_name]["rows"]:
            raise ValueError(f"Gold count reconciliation failed: {table_name}")
        if persisted.select(primary_key).distinct().count() != fact_counts[table_name]:
            raise ValueError(f"Duplicate Gold identity: {table_name}")

    return fact_counts
