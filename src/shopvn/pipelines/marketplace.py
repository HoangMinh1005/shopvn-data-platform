"""Versioned file scopes, isolated candidates and DQ-gated atomic Finance publication.

Gold/Silver writes during publish are not a multi-table transaction. Analytics
consumers use only the single certified mart committed LAST. Its immutable registry
snapshot selects active file versions, including after a failed intermediate write.
"""

import hashlib
import json
import re
from collections import Counter
from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table
from shopvn.common.spark_runtime import acquire_writer_lock, session
from shopvn.common.time import utc_instant
from shopvn.config import ProjectPaths
from shopvn.contracts.marketplace import file_name, selection
from shopvn.marts.marketplace import build_mart
from shopvn.quality.finance import assert_fin02_matches, queried_fin02
from shopvn.transformations.gold.marketplace import build_lines
from shopvn.transformations.silver.marketplace import read_active

TARGET = "shopvn.analytics.mart_commerce_finance_daily"
FACT_TARGET = "shopvn.gold.fact_sales_line"
SILVER_TARGET = "shopvn.silver.marketplace_sales_line"
ORDER_TARGET = "shopvn.gold.fact_order"
PRODUCT_TARGET = "shopvn.gold.dim_product"
DIRECT_MART_TARGET = "shopvn.analytics.mart_fin02_daily"
BRONZE_TARGET = "shopvn.bronze.marketplace_sales_line"
CHANNEL_TARGET = "shopvn.gold.dim_channel"
DATE_TARGET = "shopvn.gold.dim_date"


def snapshot(spark, table):
    if not spark.catalog.tableExists(table):
        return None
    return (
        spark.table(table + ".snapshots")
        .orderBy(F.col("committed_at").desc())
        .first()["snapshot_id"]
    )


def table_names(key):
    if not re.fullmatch(r"[a-f0-9]{24}", key):
        raise ValueError("Invalid marketplace run key")
    return {
        name: f"shopvn.staging.marketplace_{name}_{key}"
        for name in ("registry", "silver", "facts", "mart")
    }


def folder(key):
    table_names(key)
    path = ProjectPaths.from_env().runtime / "marketplace/runs" / key
    path.mkdir(parents=True, exist_ok=True)
    return path


def registry_records(manifest, prior=()):
    records = {(row["partner"], str(row["business_date"])): dict(row) for row in prior}
    for partner, day in selection(manifest["start"], manifest["end"]):
        records.setdefault(
            (partner, day),
            {
                "partner": partner,
                "business_date": day,
                "active_json": None,
                "attempt_json": json.dumps({"state": "not_requested"}),
            },
        )
    for entry in manifest["files"]:
        partner, day = entry["partner"], entry["business_date"]
        if entry["filename"] != file_name(partner, day) or not re.fullmatch(
            r"[a-f0-9]{32}", entry["capture_id"]
        ):
            raise ValueError("Invalid marketplace artifact identity")
        entry = {**entry, "observed_at": entry.get("observed_at", manifest["observed_at"])}
        previous = records[(partner, day)]
        if previous["active_json"]:
            old = json.loads(previous["active_json"])
            if utc_instant(old["observed_at"]) > utc_instant(entry["observed_at"]):
                raise ValueError("Refuse stale marketplace scope observation")
            if (
                utc_instant(old["observed_at"]) == utc_instant(entry["observed_at"])
                and entry["state"] == "verified"
                and old["sha256"] != entry["sha256"]
            ):
                raise ValueError("Same-clock marketplace conflict")
        records[(partner, day)] = {
            **previous,
            "attempt_json": json.dumps(entry),
            "active_json": json.dumps(entry)
            if entry["state"] == "verified"
            else previous["active_json"],
        }
    output = []
    for row in records.values():
        row = dict(row)
        active = json.loads(row["active_json"]) if row["active_json"] else None
        state = json.loads(row["attempt_json"])["state"]
        row["coverage_status"] = (
            "verified" if state == "verified" else ("retained_unverified" if active else state)
        )
        row["source_observed_at"] = active["observed_at"] if active else None
        output.append(row)
    return output


def validate_active(silver, active):
    observed = {
        (r.partner, str(r.order_date)): r.asDict()
        for r in silver.groupBy("partner", "order_date")
        .agg(
            F.count("*").alias("accepted"),
            F.sum(F.when(F.col("is_sales_eligible"), F.col("quantity_sold")).otherwise(0)).alias(
                "units"
            ),
            F.sum(
                F.when(
                    F.col("is_sales_eligible"), F.col("sale_price") * F.col("quantity_sold")
                ).otherwise(0)
            ).alias("merchandise"),
            F.sum(F.when(F.col("is_sales_eligible"), F.col("net_revenue")).otherwise(0)).alias(
                "reported_net"
            ),
            F.countDistinct(F.when(F.col("is_sales_eligible"), F.col("external_order_id"))).alias(
                "eligible_orders"
            ),
        )
        .collect()
    }
    for item in active:
        actual = observed.get((item["partner"], item["business_date"]), {})
        for field in ("accepted", "units", "merchandise", "reported_net", "eligible_orders"):
            if Decimal(str(actual.get(field, 0) or 0)) != Decimal(str(item["reference"][field])):
                raise ValueError("Independent marketplace source reconciliation failed: " + field)


def prepare(capture_id, key):
    paths, tables, root = ProjectPaths.from_env(), table_names(key), folder(key)
    if not re.fullmatch(r"[a-f0-9]{32}", capture_id):
        raise ValueError("Invalid capture ID")
    manifest = json.loads(
        (paths.runtime / "marketplace/landing" / capture_id / "manifest.json").read_text()
    )
    if (
        manifest["contract_version"] != 1
        or manifest["data_origin"] != "source"
        or manifest["rule_version"] != "1.1"
    ):
        raise ValueError("Require source marketplace contract v1/business rules 1.1")
    spark, lock = session("shopvn-marketplace-candidate"), None
    report = {"status": "running", "capture_id": capture_id, "run_key": key}
    try:
        lock = acquire_writer_lock(spark)
        for namespace in ("bronze", "silver", "gold", "analytics", "staging"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS shopvn.{namespace}")
        parent = snapshot(spark, TARGET)
        prior = []
        if parent is not None:
            header = (
                spark.table(TARGET)
                .select("publication_id", "registry_snapshot_id")
                .distinct()
                .collect()
            )
            if len(header) != 1:
                raise ValueError("Ambiguous certified marketplace registry")
            prior_table = table_names(header[0][0])["registry"]
            prior = [
                r.asDict()
                for r in spark.read.format("iceberg")
                .option("snapshot-id", str(header[0][1]))
                .load(prior_table)
                .collect()
            ]
        records = registry_records(manifest, prior)
        registry = spark.createDataFrame(
            [
                (
                    r["partner"],
                    date.fromisoformat(str(r["business_date"])),
                    r["active_json"],
                    r["attempt_json"],
                    r["coverage_status"],
                    r["source_observed_at"],
                )
                for r in records
            ],
            "partner string,business_date date,active_json string,attempt_json string,coverage_status string,source_observed_at string",
        ).withColumn("source_observed_at", F.to_timestamp("source_observed_at"))
        active = [json.loads(r["active_json"]) for r in records if r["active_json"]]
        silver = read_active(spark, active, paths, bronze_table=BRONZE_TARGET).cache()
        validate_active(silver, active)
        pins = {
            t: snapshot(spark, t)
            for t in (
                ORDER_TARGET,
                FACT_TARGET,
                PRODUCT_TARGET,
                DIRECT_MART_TARGET,
            )
        }
        if any(v is None for v in pins.values()):
            raise ValueError("Direct Finance baseline must be available")
        orders, direct_lines = (
            spark.table(ORDER_TARGET),
            spark.table(FACT_TARGET),
        )
        direct_actual = queried_fin02(
            [
                [str(r.order_date), r.channel, r.orders, str(r.amount)]
                for r in orders.filter(
                    "is_revenue_eligible AND source_system='postgres/shopvn' AND data_origin='source'"
                )
                .groupBy("order_date", "channel")
                .agg(F.count("*").alias("orders"), F.sum("net_customer_amount").alias("amount"))
                .collect()
            ]
        )
        direct_certified = queried_fin02(
            [
                [str(r.order_date), r.channel, r.eligible_order_count, str(r.net_customer_amount)]
                for r in spark.table(DIRECT_MART_TARGET).collect()
            ]
        )
        assert_fin02_matches(direct_actual, direct_certified)
        facts = build_lines(
            spark,
            silver,
            manifest["observed_at"],
            root / "stage",
            product_table=PRODUCT_TARGET,
            channel_table=CHANNEL_TARGET,
            date_table=DATE_TARGET,
        ).withColumn("run_id", F.lit(key))
        retained = direct_lines.filter(~F.col("source_system").startswith("marketplace/"))
        facts = retained.unionByName(facts, allowMissingColumns=True)
        if facts.select("sales_line_key").distinct().count() != facts.count():
            raise ValueError("Conformed sales-line identity conflict")
        mart = build_mart(
            spark, silver, registry, orders, direct_lines, channel_table=CHANNEL_TARGET
        )
        # Isolated candidates only. The exact snapshots below are the approval boundary.
        for name, data in (("registry", registry), ("silver", silver), ("facts", facts)):
            replace_table(spark, data, tables[name], root / "stage")
        registry_snapshot = snapshot(spark, tables["registry"])
        mart = (
            mart.withColumn("publication_id", F.lit(key))
            .withColumn("registry_snapshot_id", F.lit(registry_snapshot).cast("long"))
            .withColumn("data_as_of", F.to_timestamp(F.lit(manifest["observed_at"])))
            .withColumn("prepared_at", F.current_timestamp())
            .withColumn("published_at", F.lit(None).cast("timestamp"))
            .withColumn("rule_version", F.lit("1.1"))
        )
        replace_table(spark, mart, tables["mart"], root / "stage")
        report.update(
            status="passed",
            parent_snapshot_id=parent,
            observed_at=manifest["observed_at"],
            tables={
                name: {"name": t, "snapshot_id": snapshot(spark, t)} for name, t in tables.items()
            },
            pinned_inputs=pins,
            states=manifest["states"],
            coverage=dict(Counter(r["coverage_status"] for r in records)),
            accepted_rows=silver.count(),
            excluded_status_rows=silver.filter(~F.col("is_sales_eligible")).count(),
            formula_warnings=silver.filter(F.col("formula_delta_vnd") != 0).count(),
            marketplace_rows=facts.filter("source_system LIKE 'marketplace/%'").count(),
            expected=[
                {
                    "partner": item["partner"],
                    "business_date": item["business_date"],
                    **item["reference"],
                }
                for item in active
            ],
        )
        return report
    except Exception as exc:
        report.update(status="failed", error_class=type(exc).__name__)
        raise
    finally:
        (root / "candidate.json").write_text(
            json.dumps(report, default=str, indent=2), encoding="utf-8"
        )
        if lock is not None:
            lock.close()
        spark.stop()


def publish(key):
    root = folder(key)
    candidate = json.loads((root / "candidate.json").read_text())
    approval = json.loads((root / "validation.json").read_text())
    encoded = json.dumps(candidate, sort_keys=True).encode()
    if (
        not approval["passed"]
        or approval["candidate_sha256"] != hashlib.sha256(encoded).hexdigest()
    ):
        raise ValueError("Marketplace requires approval of the exact candidate report")
    spark, lock = session("shopvn-marketplace-publish"), None
    try:
        lock = acquire_writer_lock(spark)
        if spark.catalog.tableExists(TARGET) and spark.table(TARGET).select(
            "publication_id"
        ).distinct().collect() == [(key,)]:
            prior = spark.table(TARGET)
            expected_snapshot = candidate["tables"]["mart"]["snapshot_id"]
            if "source_candidate_snapshot_id" not in prior.columns or prior.select(
                "source_candidate_snapshot_id"
            ).distinct().collect() != [(expected_snapshot,)]:
                raise ValueError("Completed publication uses a different candidate; use a new run")
            result = {
                "status": "published",
                "idempotent_skip": True,
                "mart_snapshot_id": snapshot(spark, TARGET),
            }
            (root / "publication.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            return result
        if snapshot(spark, TARGET) != candidate["parent_snapshot_id"]:
            raise ValueError("Marketplace parent publication changed; reprepare/revalidate")
        frames = {}
        for name, item in candidate["tables"].items():
            if snapshot(spark, item["name"]) != item["snapshot_id"]:
                raise ValueError("Candidate changed after validation")
            frames[name] = (
                spark.read.format("iceberg")
                .option("snapshot-id", str(item["snapshot_id"]))
                .load(item["name"])
            )
        for table, pinned in candidate["pinned_inputs"].items():
            if snapshot(spark, table) != pinned:
                current = spark.table(table)
                own_partial_write = table == FACT_TARGET and set(current.columns) == set(
                    frames["facts"].columns
                )
                if own_partial_write:
                    expected = frames["facts"].select(current.columns)
                    own_partial_write = not (
                        current.exceptAll(expected).limit(1).count()
                        or expected.exceptAll(current).limit(1).count()
                    )
                if not own_partial_write:
                    raise ValueError("Reference input changed after validation")
        replace_table(spark, frames["silver"], SILVER_TARGET, root / "publish")
        replace_table(spark, frames["facts"], FACT_TARGET, root / "publish")
        # Single certified Finance table is committed last; its pinned registry is authoritative.
        replace_table(
            spark,
            frames["mart"]
            .withColumn("published_at", F.current_timestamp())
            .withColumn(
                "source_candidate_snapshot_id",
                F.lit(candidate["tables"]["mart"]["snapshot_id"]).cast("long"),
            ),
            TARGET,
            root / "publish",
        )
        report = {
            "status": "published",
            "idempotent_skip": False,
            "mart_snapshot_id": snapshot(spark, TARGET),
        }
        (root / "publication.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        if lock is not None:
            lock.close()
        spark.stop()
