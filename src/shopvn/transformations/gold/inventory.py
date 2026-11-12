"""Actual source stock: current observations and immutable first accepted EOD capture."""

from datetime import date

from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table
from shopvn.common.time import utc_instant
from shopvn.contracts.inventory import POLICY_VERSION, VN
from shopvn.quality.spark import fail_if
from shopvn.transformations.gold.dimensions import ensure_dates
from shopvn.transformations.gold.temporal import lookup_day_versions


def persist_policy(spark, policy, observed_at, stage, table="shopvn.ops.inventory_policy"):
    if spark.catalog.tableExists(table):
        if spark.table(table).count() != 1:
            raise ValueError("Inventory policy table must contain exactly one immutable policy")
        saved = spark.table(table).first()
        if (
            saved["go_live_date"] != policy.go_live_date
            or saved["window_seconds"] != policy.window_seconds
            or saved["policy_version"] != POLICY_VERSION
        ):
            raise ValueError("Inventory policy is immutable; explicit migration is required")
    else:
        if utc_instant(observed_at).astimezone(VN).date() != policy.go_live_date:
            raise ValueError(
                "First actual inventory observation must establish go-live today; no retroactive go-live"
            )
        data = spark.createDataFrame(
            [(policy.go_live_date, policy.window_seconds, POLICY_VERSION)],
            "go_live_date date,window_seconds int,policy_version string",
        )
        replace_table(
            spark,
            data.withColumn("first_observed_at", F.to_timestamp(F.lit(observed_at))),
            table,
            stage,
        )
    return policy


def warehouse_dimension(spark, inventory, stage, table="shopvn.gold.dim_warehouse"):
    warehouses = inventory.select("warehouse_id").distinct()
    fail_if(warehouses, F.col("warehouse_id").isNull(), "Missing inventory warehouse")
    if spark.catalog.tableExists(table):
        prior = spark.table(table)
        new = warehouses.join(prior.select("warehouse_id"), "warehouse_id", "left_anti")
        maximum = prior.agg(F.max("warehouse_key")).first()[0] or 0
    else:
        prior = spark.createDataFrame([(0, "unknown")], "warehouse_key long,warehouse_id string")
        new, maximum = warehouses, 0
    from pyspark.sql import Window

    new = new.withColumn(
        "warehouse_key",
        (F.row_number().over(Window.orderBy("warehouse_id")) + maximum).cast("long"),
    )
    replace_table(spark, prior.unionByName(new), table, stage)
    return spark.table(table)


def build_current_stock(spark, inventory, products, observed_at, stage):
    day = utc_instant(observed_at).astimezone(VN).date()
    requests = inventory.select("product_id").withColumn("order_date", F.lit(day))
    lookup = lookup_day_versions(requests, products, "product_id", "product_key", observed_at).drop(
        "order_date"
    )
    warehouses = warehouse_dimension(spark, inventory, stage)
    fail_if(
        inventory,
        F.col("stock_qty").isNull() | (F.col("stock_qty") < 0),
        "Invalid current stock quantity",
    )
    if inventory.select("product_id", "warehouse_id").distinct().count() != inventory.count():
        raise ValueError("Duplicate product/warehouse inventory identity")
    stock = (
        inventory.join(lookup, "product_id", "left")
        .join(warehouses, "warehouse_id", "left")
        .withColumn("observed_at", F.to_timestamp(F.lit(observed_at)))
        .withColumnRenamed("last_updated", "source_last_updated")
        .withColumn("snapshot_method", F.lit("current_observed"))
        .withColumn("data_origin", F.lit("source"))
    )
    fail_if(
        stock,
        F.col("product_key").isNull() | F.col("warehouse_key").isNull(),
        "Missing stock dimension lookup",
    )
    ensure_dates(spark, spark.createDataFrame([(day,)], "full_date date"), stage)
    return stock


def publish_stock(
    spark,
    stock,
    manifest,
    capture,
    stage,
    *,
    current="shopvn.analytics.current_stock",
    daily="shopvn.gold.fact_inventory_daily_snapshot",
    update_calendar=True,
):
    expected = {
        row["warehouse_id"]: (int(row["rows"]), int(row["stock_qty"]))
        for row in manifest["inventory_expected"]
    }
    actual = {
        row["warehouse_id"]: (row["rows"], row["stock_qty"])
        for row in stock.groupBy("warehouse_id")
        .agg(F.count("*").alias("rows"), F.sum("stock_qty").alias("stock_qty"))
        .collect()
    }
    if actual != expected:
        raise ValueError("Source/current-stock warehouse reconciliation failed")
    publish_current = True
    if spark.catalog.tableExists(current):
        publish_current = (
            spark.table(current)
            .filter(F.col("observed_at") > F.to_timestamp(F.lit(manifest["observed_at_utc"])))
            .limit(1)
            .count()
            == 0
        )
    if publish_current:
        replace_table(spark, stock, current, stage)
    eod_published = False
    if capture["eod_eligible"]:
        day = date.fromisoformat(capture["business_date"])
        data = (
            stock.withColumn("snapshot_date", F.lit(day))
            .withColumn("snapshot_date_key", F.date_format(F.lit(day), "yyyyMMdd").cast("int"))
            .withColumn("snapshot_cutoff_at", F.to_timestamp(F.lit(capture["cutoff_at"])))
            .withColumn("capture_lag_seconds", F.lit(capture["capture_lag_seconds"]))
            .withColumn("snapshot_method", F.lit("observed_eod_cutoff_window"))
            .withColumn("policy_version", F.lit(capture["policy_version"]))
        )
        if spark.catalog.tableExists(daily):
            prior = spark.table(daily)
            same = prior.filter(F.col("snapshot_date") == F.lit(day))
            if same.limit(1).count():
                columns = ["product_id", "warehouse_id", "stock_qty"]
                if (
                    same.select(columns).exceptAll(data.select(columns)).limit(1).count()
                    or data.select(columns).exceptAll(same.select(columns)).limit(1).count()
                ):
                    raise ValueError(
                        "Conflicting EOD payload; first accepted actual capture is immutable"
                    )
            else:
                replace_table(spark, prior.unionByName(data), daily, stage)
        else:
            replace_table(spark, data, daily, stage)
        if update_calendar:
            ensure_dates(spark, spark.createDataFrame([(day,)], "full_date date"), stage)
        eod_published = True
    return {
        "current_published": publish_current,
        "eod_published": eod_published,
        "rows": sum(value[0] for value in actual.values()),
        "stock_qty": sum(value[1] for value in actual.values()),
        "warehouse_reconciliation": True,
    }
