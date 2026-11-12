"""FIN04–07/10–12, separate bases and explicit missing/retained file coverage."""

from pyspark.sql import functions as F

from shopvn.contracts.marketplace import PARTNERS


def build_mart(
    spark, silver, registry, orders, direct_lines, *, channel_table="shopvn.gold.dim_channel"
):
    coverage = registry.select("partner", "business_date", "coverage_status", "source_observed_at")
    eligible = silver.filter("is_sales_eligible")
    amounts = eligible.groupBy("partner", F.col("order_date").alias("business_date")).agg(
        F.sum(F.col("sale_price") * F.col("quantity_sold")).alias("comparable_merchandise"),
        F.sum("net_revenue").alias("partner_reported_net"),
        F.countDistinct("external_order_id").alias("eligible_orders"),
        F.sum("quantity_sold").alias("units_sold"),
    )
    trade = coverage.join(amounts, ["partner", "business_date"], "left")
    # A verified empty/zero-completed file is zero; missing file is NULL, never zero.
    for column in (
        "comparable_merchandise",
        "partner_reported_net",
        "eligible_orders",
        "units_sold",
    ):
        trade = trade.withColumn(
            column,
            F.when(
                F.col("coverage_status").isin("verified", "retained_unverified"),
                F.coalesce(F.col(column), F.lit(0)),
            ).otherwise(F.lit(None)),
        )
    trade = (
        trade.withColumn("channel", F.col("partner"))
        .withColumn("report_date", F.col("business_date"))
        .withColumn("report_type", F.lit("sales"))
        .withColumn(
            "implied_partner_deduction",
            F.col("comparable_merchandise") - F.col("partner_reported_net"),
        )
        .withColumn("direct_net_customer_amount", F.lit(None).cast("decimal(20,2)"))
    )
    settlement = trade.withColumn("report_date", F.date_add("business_date", 15)).withColumn(
        "report_type", F.lit("scheduled_settlement")
    )
    for column in (
        "comparable_merchandise",
        "implied_partner_deduction",
        "eligible_orders",
        "units_sold",
    ):
        settlement = settlement.withColumn(column, F.lit(None).cast(trade.schema[column].dataType))
    direct = (
        orders.filter(
            "is_revenue_eligible AND source_system='postgres/shopvn' AND data_origin='source'"
        )
        .groupBy(
            F.col("order_date").alias("business_date"),
            F.concat(F.lit("direct_"), "channel").alias("channel"),
        )
        .agg(
            F.sum("merchandise_amount").alias("comparable_merchandise"),
            F.sum("net_customer_amount").alias("direct_net_customer_amount"),
            F.countDistinct("source_order_id").alias("eligible_orders"),
        )
    )
    units = (
        direct_lines.filter(
            "is_sales_eligible AND source_system='postgres/shopvn' AND data_origin='source'"
        )
        .groupBy(
            F.to_date(F.col("order_date_key").cast("string"), "yyyyMMdd").alias("business_date"),
            "channel_key",
        )
        .agg(F.sum("quantity").alias("units_sold"))
        .join(
            spark.table(channel_table).select(
                "channel_key", F.col("channel_code").alias("channel")
            ),
            "channel_key",
        )
    )
    direct = (
        direct.join(
            units.select("business_date", "channel", "units_sold"),
            ["business_date", "channel"],
            "left",
        )
        .withColumn("report_date", F.col("business_date"))
        .withColumn("report_type", F.lit("sales"))
        .withColumn("coverage_status", F.lit("verified_snapshot"))
        .withColumn(
            "source_observed_at",
            F.lit(orders.agg(F.max("observed_at")).first()[0]).cast("timestamp"),
        )
    )
    columns = [
        "business_date",
        "report_date",
        "channel",
        "report_type",
        "coverage_status",
        "source_observed_at",
        "comparable_merchandise",
        "partner_reported_net",
        "implied_partner_deduction",
        "direct_net_customer_amount",
        "eligible_orders",
        "units_sold",
    ]
    for column in ("partner_reported_net", "implied_partner_deduction"):
        direct = direct.withColumn(column, F.lit(None).cast("decimal(20,2)"))
    result = (
        trade.select(columns)
        .unionByName(settlement.select(columns))
        .unionByName(direct.select(columns))
    )
    completeness = coverage.groupBy("business_date").agg(
        (
            (F.count("*") == len(PARTNERS))
            & F.min((F.col("coverage_status") == "verified").cast("int")).cast("boolean")
        ).alias("marketplace_day_complete")
    )
    result = result.join(completeness, "business_date", "left").fillna(
        {"marketplace_day_complete": False}
    )
    result = result.withColumn(
        "aov_merchandise",
        F.when(
            F.col("eligible_orders") > 0, F.col("comparable_merchandise") / F.col("eligible_orders")
        ).cast("decimal(20,4)"),
    )
    for column in (
        "comparable_merchandise",
        "partner_reported_net",
        "implied_partner_deduction",
        "direct_net_customer_amount",
    ):
        result = result.withColumn(column, F.col(column).cast("decimal(20,2)"))
    return result.withColumn("currency_code", F.lit("VND")).withColumn(
        "data_origin", F.lit("source")
    )
