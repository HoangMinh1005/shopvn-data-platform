"""Capture actual stock independently of the historical Finance business-date window."""

import json
import re
import uuid

from shopvn.common.spark_runtime import acquire_writer_lock, session
from shopvn.config import ProjectPaths
from shopvn.contracts.direct import SCHEMAS
from shopvn.contracts.inventory import InventoryPolicy
from shopvn.quality.temporal import assert_observation_order
from shopvn.transformations.bronze.direct import load_table
from shopvn.transformations.gold.inventory import build_current_stock, persist_policy, publish_stock
from shopvn.transformations.gold.temporal import observed_dimension
from shopvn.transformations.silver.direct import materialize, normalize


def run(snapshot_id, *, mode="current", business_date=None, result_path=None):
    paths = ProjectPaths.from_env()
    if not re.fullmatch(r"[a-f0-9]{32}", snapshot_id):
        raise ValueError("Invalid inventory snapshot ID")
    manifest = json.loads(
        (paths.landing / snapshot_id / "manifest.json").read_text(encoding="utf-8")
    )
    if (
        not {"inventory", "products"} <= set(manifest["tables"])
        or manifest.get("data_origin") != "source"
    ):
        raise ValueError("Inventory publication requires actual source inventory/products")
    observed = manifest["observed_at_utc"]
    policy = InventoryPolicy.from_env()
    capture = policy.classify(observed, mode=mode, business_date=business_date)
    result_file = paths.result_file(result_path or paths.runtime / "inventory/latest-result.json")
    result_file.parent.mkdir(parents=True, exist_ok=True)
    stage = paths.runtime / "inventory/stage" / uuid.uuid4().hex
    spark = session("shopvn-actual-inventory")
    lock = None
    report = {"status": "running", "snapshot_id": snapshot_id, "capture": capture}
    try:
        lock = acquire_writer_lock(spark)
        for name in ("ops", "bronze", "silver", "gold", "analytics"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS shopvn.{name}")
        persist_policy(spark, policy, observed, stage)
        bronze = {
            name: load_table(spark, name, SCHEMAS[name], manifest, snapshot_id, paths.landing)
            for name in ("inventory", "products")
        }
        table = "shopvn.gold.dim_product"
        try:
            assert_observation_order(spark, observed, (table,))
            update_dimension = True
        except ValueError:
            update_dimension = False
        frames = (
            materialize(spark, bronze, stage)
            if update_dimension
            else {name: normalize(name, frame) for name, frame in bronze.items()}
        )
        spark.catalog.clearCache()
        if update_dimension:
            incoming = (
                frames["products"]
                .drop("source_record_hash", "snapshot_id")
                .withColumnRenamed("name", "product_name")
            )
            products = observed_dimension(
                spark, incoming, table, "product_key", "product_id", observed, stage
            )
        else:
            # A saved actual EOD capture may be processed after newer observations.
            # Resolve against existing history; never backdate/rewrite the dimension.
            products = spark.table(table)
        stock = build_current_stock(spark, frames["inventory"], products, observed, stage)
        report.update(publish_stock(spark, stock, manifest, capture, stage))
        if mode == "eod" and not capture["eod_eligible"]:
            raise ValueError("EOD cutoff missed; only current stock was published")
        report["status"] = "passed"
        return report
    except Exception as exc:
        report.update(status="failed", error_class=type(exc).__name__)
        raise
    finally:
        try:
            result_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
        finally:
            if lock is not None:
                lock.close()
            spark.stop()
