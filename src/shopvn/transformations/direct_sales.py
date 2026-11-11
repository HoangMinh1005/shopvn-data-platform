"""First direct Finance vertical slice; observed dimensions, source-cohort FIN02."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopvn.common.spark_runtime import acquire_writer_lock, session

MONEY = "decimal(20,2)"
SCHEMAS = {
    "orders": "order_id string,customer_id int,voucher_code string,channel string,order_date date,status string,subtotal decimal(20,2),shipping_fee decimal(20,2),discount_amount decimal(20,2),total_amount decimal(20,2),payment_method string,payment_status string,created_at timestamp_ntz",
    "order_items": "item_id int,order_id string,product_id int,quantity int,unit_price decimal(20,2),discount_per_item decimal(20,2),total_price decimal(20,2)",
    "returns": "return_id int,order_id string,reason string,status string,refund_amount decimal(20,2),created_at timestamp_ntz",
    "customers": "customer_id int,loyalty_tier string,gender string,city string",
    "products": "product_id int,sku string,name string,category string,subcategory string,brand string,base_price decimal(20,2),cost_price decimal(20,2),weight_gram int,is_active string",
    "vouchers": "voucher_code string,type string,value decimal(20,4),min_order_value decimal(20,2),max_discount decimal(20,2),campaign_id string,valid_from date,valid_to date,max_usage int",
}
PRIMARY_KEYS = {
    "orders": "order_id",
    "order_items": "item_id",
    "returns": "return_id",
    "customers": "customer_id",
    "products": "product_id",
    "vouchers": "voucher_code",
}


def fail_if(df, predicate, description: str):
    if df.filter(predicate).limit(1).count():
        raise ValueError(description)


def with_hash(df, name: str, columns=None):
    fields = sorted(columns or df.columns)
    return df.withColumn(
        name, F.sha2(F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}), 256)
    )


def replace_table(spark, df, table: str, stage: Path):
    # Break self-read lineage before atomic RTAS of an existing Iceberg dimension.
    path = str(stage / table.replace(".", "_"))
    df.coalesce(2).write.mode("overwrite").parquet(path)
    (
        spark.read.parquet(path)
        .writeTo(table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .createOrReplace()
    )


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


def merge_fact(spark, df, table: str, key: str, observed_at: str, run_id: str):
    business_columns = [c for c in df.columns if c not in {"snapshot_id", "source_record_hash"}]
    df = (
        with_hash(df, "business_hash", business_columns)
        .withColumn("observed_at", F.to_timestamp(F.lit(observed_at)))
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_system", F.lit("postgres/shopvn"))
        .withColumn("data_origin", F.lit("source"))
    )
    if not spark.catalog.tableExists(table):
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
    else:
        df.createOrReplaceTempView("incoming_fact")
        spark.sql(f"""MERGE INTO {table} t USING incoming_fact s ON t.{key}=s.{key}
                  WHEN MATCHED AND t.business_hash<>s.business_hash THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")


def run(
    snapshot_id: str,
    *,
    candidate_name: str | None = None,
    result_path: str | None = None,
    publication_id: str | None = None,
):
    root = Path("/workspace/runtime/direct")
    manifest = json.loads((root / "landing" / snapshot_id / "manifest.json").read_text())
    if set(manifest["tables"]) != set(SCHEMAS):
        raise ValueError("Extract a new snapshot with all six source tables")
    observed_at = manifest["observed_at_utc"]
    run_id = publication_id or uuid.uuid4().hex
    if candidate_name and not re.fullmatch(r"mart_fin02_[a-f0-9]{24}", candidate_name):
        raise ValueError("Invalid isolated candidate table name")
    result_file = Path(result_path) if result_path else root / "direct-result.json"
    if not result_file.resolve().is_relative_to(Path("/workspace/runtime").resolve()):
        raise ValueError("Result path must stay inside runtime")
    result_file.parent.mkdir(parents=True, exist_ok=True)
    stage = root / "stage" / run_id
    spark = session("shopvn-direct-finance")
    writer_lock = None
    report = {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "rule_version": "1.1",
        "status": "running",
    }
    try:
        writer_lock = acquire_writer_lock(spark)
        for namespace in ("bronze", "silver", "gold", "analytics", "staging"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS shopvn.{namespace}")
        raw = {}
        for name, schema in SCHEMAS.items():
            path = root / "landing" / snapshot_id / f"{name}.csv"
            with path.open("rb") as stream:
                if (
                    hashlib.file_digest(stream, "sha256").hexdigest()
                    != manifest["tables"][name]["sha256"]
                ):
                    raise ValueError(f"Source snapshot checksum failed: {name}")
            frame = (
                spark.read.schema(schema)
                .option("header", True)
                .option("mode", "FAILFAST")
                .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSS]")
                .csv(str(path))
                .cache()
            )
            key = PRIMARY_KEYS[name]
            count = frame.count()
            if (
                count != manifest["tables"][name]["rows"]
                or frame.select(key).distinct().count() != count
            ):
                raise ValueError(f"Count/identity reconciliation failed: {name}")
            fail_if(frame, F.col(key).isNull(), f"Null source identity: {name}")
            frame = with_hash(frame, "source_record_hash").withColumn(
                "snapshot_id", F.lit(snapshot_id)
            )
            bronze = f"shopvn.bronze.direct_{name}"
            if not spark.catalog.tableExists(bronze):
                frame.writeTo(bronze).using("iceberg").tableProperty("format-version", "2").create()
            elif spark.table(bronze).filter(F.col("snapshot_id") == snapshot_id).count() == 0:
                frame.writeTo(bronze).append()
            persisted_count = (
                spark.table(bronze).filter(F.col("snapshot_id") == snapshot_id).count()
            )
            if persisted_count != count:
                raise ValueError(f"Bronze replay count mismatch: {name}")
            silver_frame = frame
            if name == "products":
                # PostgreSQL COPY encodes booleans t/f; preserve the wire value in Bronze.
                fail_if(frame, ~F.col("is_active").isin("t", "f", "true", "false"),
                        "Unknown PostgreSQL boolean encoding")
                silver_frame = frame.withColumn("is_active", F.col("is_active").isin("t", "true"))
            replace_table(spark, silver_frame, f"shopvn.silver.direct_{name}", stage)
            raw[name] = spark.table(f"shopvn.silver.direct_{name}")

        orders, lines, returns = raw["orders"], raw["order_items"], raw["returns"]
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
        secret = (root / "hmac.key").read_bytes()
        customer_ref = F.udf(
            lambda value: hmac.new(
                secret, f"postgres:{value}".encode(), hashlib.sha256
            ).hexdigest(),
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
            .withColumn(
                "net_customer_amount", F.col("total_amount") - F.col("refunded_order_amount")
            )
            .withColumn(
                "merchandise_less_order_refund_proxy",
                F.col("merchandise_amount") - F.col("refunded_order_amount"),
            )
        )
        fail_if(enriched, F.col("customer_key") == 0, "Missing customer dimension lookup")
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
        merge_fact(spark, order_fact, "shopvn.gold.fact_order", "order_key", observed_at, run_id)

        line_base = lines.join(
            enriched.select(
                "order_id",
                "customer_key",
                "channel_key",
                "order_date_key",
                "subtotal",
                "discount_amount",
                "is_revenue_eligible",
                "has_excess_discount",
            ),
            "order_id",
        ).join(earliest_product, "product_id", "left")
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
            F.sha2(F.concat(F.lit("postgres:item:"), F.col("item_id")), 256).alias(
                "sales_line_key"
            ),
            F.col("item_id").cast("string").alias("source_line_id"),
            F.col("order_id").alias("source_order_id"),
            "product_key",
            "customer_key",
            "channel_key",
            "order_date_key",
            "quantity",
            (F.col("unit_price") - F.col("discount_per_item")).alias("unit_sale_price"),
            (F.col("discount_per_item") * F.col("quantity"))
            .cast(MONEY)
            .alias("item_discount_amount"),
            F.col("total_price").alias("line_merchandise_before_header_discount"),
            "allocated_header_discount_amount",
            (F.col("total_price") - F.col("allocated_header_discount_amount"))
            .cast(MONEY)
            .alias("net_merchandise_before_refund"),
            (F.col("is_revenue_eligible") & (F.col("quantity") > 0)).alias("is_sales_eligible"),
            "source_record_hash",
            "snapshot_id",
        )
        merge_fact(
            spark, line_fact, "shopvn.gold.fact_sales_line", "sales_line_key", observed_at, run_id
        )
        line_recon = line_fact.groupBy("source_order_id").agg(
            F.sum("net_merchandise_before_refund").alias("line_merchandise")
        )
        fail_if(
            order_fact.filter("is_revenue_eligible").join(line_recon, "source_order_id"),
            F.col("merchandise_amount") != F.col("line_merchandise"),
            "Allocated line-to-header mismatch",
        )

        return_fact = returns.join(
            enriched.select("order_id", "customer_key", "channel_key", "order_date_key"), "order_id"
        ).select(
            F.sha2(F.concat(F.lit("postgres:return:"), F.col("return_id")), 256).alias(
                "return_key"
            ),
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
        merge_fact(spark, return_fact, "shopvn.gold.fact_return", "return_key", observed_at, run_id)
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
        mart = (
            spark.table("shopvn.gold.fact_order")
            .filter("is_revenue_eligible")
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
        actual = {
            (str(row.order_date), row.channel): (row.eligible_order_count, row.net_customer_amount)
            for row in mart.collect()
        }
        expected = {
            (row["order_date"], row["channel"]): (
                row["eligible_order_count"],
                Decimal(row["net_customer_amount"]),
            )
            for row in manifest["fin02_expected_by_day_channel"]
        }
        if actual != expected:
            raise ValueError(
                "FIN02 source-to-Gold reconciliation failed; certified mart not replaced"
            )
        target = (
            f"shopvn.staging.{candidate_name}"
            if candidate_name else "shopvn.analytics.mart_fin02_daily"
        )
        replace_table(
            spark,
            mart.withColumn("rule_version", F.lit("1.1"))
            .withColumn("data_as_of", F.to_timestamp(F.lit(observed_at)))
            .withColumn("publication_id", F.lit(run_id)),
            target,
            stage,
        )
        candidate_snapshot = (
            spark.table(target + ".snapshots")
            .orderBy(F.col("committed_at").desc()).first()["snapshot_id"]
        )
        report.update(
            status="passed",
            fin02_total=str(sum(value[1] for value in actual.values())),
            eligible_orders=sum(value[0] for value in actual.values()),
            mart_rows=len(actual),
            fact_counts=fact_counts,
            source_counts={name: value["rows"] for name, value in manifest["tables"].items()},
            excluded_discount_orders=orders.filter(
                F.col("discount_amount") > F.col("subtotal")
            ).count(),
            zero_quantity_lines=lines.filter("quantity=0").count(),
            target_table=target,
            candidate_snapshot_id=candidate_snapshot,
            published=candidate_name is None,
        )
        print("DIRECT_RESULT=" + json.dumps(report), flush=True)
    except Exception as exc:
        report.update(status="failed", failure=type(exc).__name__ + ": " + str(exc)[:1000])
        raise
    finally:
        result_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
        if writer_lock is not None:
            writer_lock.close()
        spark.stop()
