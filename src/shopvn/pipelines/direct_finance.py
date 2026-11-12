"""Direct Finance batch workflow; layer modules own business transformations."""

import json
import re
import uuid

from pyspark.sql import functions as F

from shopvn.common.iceberg import merge_fact, replace_table
from shopvn.common.spark_runtime import acquire_writer_lock, session
from shopvn.config import ProjectPaths
from shopvn.contracts.direct import CONTRACT_VERSION, RULE_VERSION, SCHEMAS
from shopvn.marts.finance import build_fin02
from shopvn.quality.direct import (
    validate_allocated_lines,
    validate_gold_counts,
    validate_silver,
)
from shopvn.quality.finance import assert_fin02_matches, expected_fin02
from shopvn.quality.temporal import assert_observation_order
from shopvn.transformations.bronze.direct import load_table
from shopvn.transformations.gold.dimensions import build_dimensions
from shopvn.transformations.gold.orders import build_order_fact, enrich_orders
from shopvn.transformations.gold.returns import build_return_fact
from shopvn.transformations.gold.sales_lines import build_sales_line_fact
from shopvn.transformations.silver.direct import materialize


def run(snapshot_id, *, candidate_name=None, result_path=None, publication_id=None, paths=None):
    paths = paths or ProjectPaths.from_env()
    if not re.fullmatch(r"[a-f0-9]{32}", snapshot_id):
        raise ValueError("Invalid landing snapshot identifier")
    if candidate_name and not re.fullmatch(r"mart_fin02_[a-f0-9]{24}", candidate_name):
        raise ValueError("Invalid isolated candidate table name")
    run_id = publication_id or uuid.uuid4().hex
    if not re.fullmatch(r"[a-f0-9]{24}|[a-f0-9]{32}", run_id):
        raise ValueError("Invalid publication identifier")
    manifest = json.loads(
        (paths.landing / snapshot_id / "manifest.json").read_text(encoding="utf-8")
    )
    if (
        set(manifest["tables"]) != set(SCHEMAS)
        or manifest["rule_version"] != RULE_VERSION
        or manifest.get("contract_version") != CONTRACT_VERSION
    ):
        raise ValueError(
            "Extract a snapshot with all nine source tables and the current rule version"
        )
    observed_at = manifest["observed_at_utc"]
    result_file = paths.result_file(result_path)
    result_file.parent.mkdir(parents=True, exist_ok=True)
    stage = paths.direct / "stage" / run_id
    report = {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "rule_version": RULE_VERSION,
        "status": "running",
    }
    spark = session("shopvn-direct-finance")
    lock = None
    try:
        lock = acquire_writer_lock(spark)
        assert_observation_order(spark, observed_at)
        for namespace in ("bronze", "silver", "gold", "analytics", "staging"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS shopvn.{namespace}")
        bronze = {
            name: load_table(spark, name, schema, manifest, snapshot_id, paths.landing)
            for name, schema in SCHEMAS.items()
        }
        frames = materialize(spark, bronze, stage)
        spark.catalog.clearCache()
        validate_silver(frames, observed_at)
        customer_ref, customers, products = build_dimensions(
            spark, frames, observed_at, stage, paths.hmac_key
        )
        enriched = enrich_orders(frames["orders"], frames["returns"], customer_ref, customers)
        orders = build_order_fact(enriched)
        merge_fact(spark, orders, "shopvn.gold.fact_order", "order_key", observed_at, run_id)
        lines = build_sales_line_fact(frames["order_items"], enriched, products)
        merge_fact(
            spark, lines, "shopvn.gold.fact_sales_line", "sales_line_key", observed_at, run_id
        )
        validate_allocated_lines(orders, lines)
        returns = build_return_fact(frames["returns"], enriched)
        merge_fact(spark, returns, "shopvn.gold.fact_return", "return_key", observed_at, run_id)
        counts = validate_gold_counts(spark, manifest)
        mart = build_fin02(spark.table("shopvn.gold.fact_order"))
        actual = {
            (str(row.order_date), row.channel): (row.eligible_order_count, row.net_customer_amount)
            for row in mart.collect()
        }
        assert_fin02_matches(actual, expected_fin02(manifest))
        target = (
            f"shopvn.staging.{candidate_name}"
            if candidate_name
            else "shopvn.analytics.mart_fin02_daily"
        )
        replace_table(
            spark,
            mart.withColumn("rule_version", F.lit(RULE_VERSION))
            .withColumn("data_as_of", F.to_timestamp(F.lit(observed_at)))
            .withColumn("publication_id", F.lit(run_id)),
            target,
            stage,
        )
        snapshot = (
            spark.table(target + ".snapshots")
            .orderBy(F.col("committed_at").desc())
            .first()["snapshot_id"]
        )
        report.update(
            status="passed",
            fin02_total=str(sum(value[1] for value in actual.values())),
            eligible_orders=sum(value[0] for value in actual.values()),
            mart_rows=len(actual),
            fact_counts=counts,
            source_counts={name: value["rows"] for name, value in manifest["tables"].items()},
            excluded_discount_orders=frames["orders"]
            .filter(F.col("discount_amount") > F.col("subtotal"))
            .count(),
            zero_quantity_lines=frames["order_items"].filter("quantity=0").count(),
            target_table=target,
            candidate_snapshot_id=snapshot,
            published=candidate_name is None,
        )
        print("DIRECT_RESULT=" + json.dumps(report), flush=True)
        return report
    except Exception as exc:
        report.update(status="failed", failure=type(exc).__name__ + ": " + str(exc)[:1000])
        raise
    finally:
        try:
            result_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
        finally:
            if lock is not None:
                lock.close()
            spark.stop()
