"""S2 synthetic persistence/semantics tests, isolated shopvn.stage2_tests tables."""

import csv
import hashlib
import json
import unittest
import uuid
from contextlib import ExitStack
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from pyspark.sql import functions as F

from shopvn.common.iceberg import merge_fact, replace_table
from shopvn.common.spark_runtime import session
from shopvn.config import ProjectPaths
from shopvn.contracts.marketplace import COLUMNS
from shopvn.marts.marketplace import build_mart
from shopvn.pipelines import marketplace as pipeline
from shopvn.transformations.gold.dimensions import ensure_channels


class MarketplaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session("shopvn-stage2-tests")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS shopvn.stage2_tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.key = uuid.uuid4().hex[:24]
        self.stage = ProjectPaths.from_env().runtime / "stage2/tests" / self.key

    def table(self, label):
        return "shopvn.stage2_tests." + label + "_" + self.key

    def record(self, sha="a", state="verified", clock="2026-11-12T08:00:00Z"):
        item = {
            "partner": "lazada",
            "business_date": "2026-06-01",
            "filename": "lazada_20260601.csv",
            "capture_id": "0" * 32,
            "state": state,
            "sha256": sha * 64,
            "observed_at": clock,
            "reference": {
                "accepted": 1,
                "units": 1,
                "merchandise": "100",
                "reported_net": "92",
                "eligible_orders": 1,
            },
        }
        return {"start": "2026-06-01", "end": "2026-06-01", "observed_at": clock, "files": [item]}

    def test_registry_replacement_and_retained_missing_and_targeted_rerun(self):
        original = pipeline.registry_records(self.record())
        self.assertEqual(len(original), 3)
        missing = pipeline.registry_records(
            self.record(state="missing", clock="2026-11-12T09:00:00Z"), original
        )
        lazada = next(r for r in missing if r["partner"] == "lazada")
        self.assertEqual(lazada["coverage_status"], "retained_unverified")
        self.assertEqual(json.loads(lazada["active_json"])["sha256"], "a" * 64)
        updated = pipeline.registry_records(
            self.record(sha="b", clock="2026-11-12T10:00:00Z"), missing
        )
        self.assertEqual(
            json.loads(next(r for r in updated if r["partner"] == "lazada")["active_json"])[
                "sha256"
            ],
            "b" * 64,
        )

    def test_registry_stale_and_same_clock_conflict_are_blocked(self):
        original = pipeline.registry_records(self.record())
        with self.assertRaisesRegex(ValueError, "stale"):
            pipeline.registry_records(self.record(clock="2026-11-12T07:00:00Z"), original)
        with self.assertRaisesRegex(ValueError, "Same-clock"):
            pipeline.registry_records(self.record(sha="b"), original)

    def test_source_aware_merge_evolves_without_erasing_other_source(self):
        table = self.table("fact")
        direct = self.spark.createDataFrame(
            [("d1", Decimal("100"))], "id string,amount decimal(20,2)"
        )
        other = self.spark.createDataFrame(
            [("m1", Decimal("92"), "paid")], "id string,amount decimal(20,2),partner_state string"
        )
        merge_fact(self.spark, direct, table, "id", "2026-11-12T08:00:00Z", self.key)
        merge_fact(
            self.spark,
            other,
            table,
            "id",
            "2026-11-12T08:00:00Z",
            self.key,
            source_system="marketplace/lazada",
        )
        merge_fact(self.spark, direct, table, "id", "2026-11-12T09:00:00Z", self.key)
        rows = {r.id: r for r in self.spark.table(table).collect()}
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows["m1"].source_system, "marketplace/lazada")
        self.assertEqual(rows["m1"].partner_state, "paid")

    def test_channel_keys_survive_direct_seed_after_marketplace(self):
        table = self.table("channels")
        replace_table(
            self.spark,
            self.spark.createDataFrame(
                [(1, "direct_web", "direct"), (99, "future", "marketplace")],
                "channel_key long,channel_code string,channel_group string",
            ),
            table,
            self.stage,
        )
        first = ensure_channels(self.spark, self.stage, table)
        expected = {r.channel_code: r.channel_key for r in first.collect()}
        self.assertEqual(expected["lazada"], 3)
        self.assertEqual(expected["future"], 99)
        self.assertEqual(
            expected,
            {
                r.channel_code: r.channel_key
                for r in ensure_channels(self.spark, self.stage, table).collect()
            },
        )

    def test_marketplace_basis_aov_and_missing_are_not_zero(self):
        silver = self.spark.createDataFrame(
            [
                ("lazada", date(2026, 6, 1), "same-order", 1, Decimal("100"), Decimal("92"), True),
                ("lazada", date(2026, 6, 1), "same-order", 1, Decimal("100"), Decimal("92"), True),
                ("lazada", date(2026, 6, 1), "returned", 1, Decimal("100"), Decimal("92"), False),
            ],
            "partner string,order_date date,external_order_id string,quantity_sold long,sale_price decimal(20,2),net_revenue decimal(20,2),is_sales_eligible boolean",
        )
        registry = self.spark.createDataFrame(
            [
                (p, date(2026, 6, 1), s, None)
                for p, s in [("lazada", "verified"), ("shopee", "missing"), ("tiktok", "verified")]
            ],
            "partner string,business_date date,coverage_status string,source_observed_at timestamp",
        )
        orders = self.spark.createDataFrame(
            [
                (
                    "same-order",
                    date(2026, 6, 1),
                    "web",
                    Decimal("50"),
                    Decimal("65"),
                    True,
                    "postgres/shopvn",
                    "source",
                    None,
                )
            ],
            "source_order_id string,order_date date,channel string,merchandise_amount decimal(20,2),net_customer_amount decimal(20,2),is_revenue_eligible boolean,source_system string,data_origin string,observed_at timestamp",
        )
        lines = self.spark.createDataFrame(
            [(20260601, 1, 1, True, "postgres/shopvn", "source")],
            "order_date_key int,channel_key long,quantity long,is_sales_eligible boolean,source_system string,data_origin string",
        )
        mart = build_mart(self.spark, silver, registry, orders, lines)
        rows = {(r.channel, r.report_type): r for r in mart.collect()}
        m = rows[("lazada", "sales")]
        self.assertEqual(
            (
                m.comparable_merchandise,
                m.partner_reported_net,
                m.eligible_orders,
                m.aov_merchandise,
            ),
            (200, 184, 1, 200),
        )
        self.assertIsNone(rows[("shopee", "sales")].partner_reported_net)
        self.assertEqual(rows[("tiktok", "sales")].partner_reported_net, 0)
        self.assertEqual(rows[("direct_web", "sales")].direct_net_customer_amount, 65)
        self.assertIsNone(rows[("lazada", "scheduled_settlement")].comparable_merchandise)
        self.assertFalse(m.marketplace_day_complete)

    def publication_fixture(self):
        # Small synthetic tables only. Patch destinations; never publish fixtures into actual Gold.
        targets = {k: self.table(k) for k in ("target", "facts_target", "silver_target")}
        replace_table(
            self.spark,
            self.spark.createDataFrame([("old", 0)], "publication_id string,id long"),
            targets["facts_target"],
            self.stage,
        )
        candidate = {
            "status": "passed",
            "parent_snapshot_id": None,
            "pinned_inputs": {
                targets["facts_target"]: pipeline.snapshot(self.spark, targets["facts_target"])
            },
            "tables": {},
            "expected": [],
        }
        for name in ("registry", "silver", "facts", "mart"):
            table = self.table(name)
            frame = self.spark.createDataFrame([(self.key, 1)], "publication_id string,id long")
            replace_table(self.spark, frame, table, self.stage)
            candidate["tables"][name] = {
                "name": table,
                "snapshot_id": pipeline.snapshot(self.spark, table),
            }
        root = pipeline.folder(self.key)
        (root / "candidate.json").write_text(json.dumps(candidate))
        approval = {
            "passed": True,
            "candidate_sha256": hashlib.sha256(
                json.dumps(candidate, sort_keys=True).encode()
            ).hexdigest(),
        }
        (root / "validation.json").write_text(json.dumps(approval))
        return targets, candidate, approval

    def test_publication_gate_partial_write_recovery_and_same_run_noop(self):
        targets, candidate, approval = self.publication_fixture()
        root = pipeline.folder(self.key)
        (root / "validation.json").write_text(json.dumps({**approval, "passed": False}))
        with self.assertRaisesRegex(ValueError, "approval"):
            pipeline.publish(self.key)
        (root / "validation.json").write_text(json.dumps(approval))
        # publish() normally owns/stops its session; tests keep the shared fixture engine alive.
        with (
            patch.object(pipeline, "TARGET", targets["target"]),
            patch.object(pipeline, "FACT_TARGET", targets["facts_target"]),
            patch.object(pipeline, "SILVER_TARGET", targets["silver_target"]),
            patch.object(pipeline, "session", return_value=self.spark),
            patch.object(self.spark, "stop"),
        ):

            def fail_before_mart(spark, data, table, stage):
                if table == targets["target"]:
                    raise RuntimeError("injected before certified commit")
                return replace_table(spark, data, table, stage)

            with patch.object(pipeline, "replace_table", side_effect=fail_before_mart):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    pipeline.publish(self.key)
            self.assertFalse(self.spark.catalog.tableExists(targets["target"]))
            self.assertTrue(self.spark.catalog.tableExists(targets["facts_target"]))
            pipeline.publish(self.key)
            before = pipeline.snapshot(self.spark, targets["target"])
            self.assertTrue(pipeline.publish(self.key)["idempotent_skip"])
            self.assertEqual(before, pipeline.snapshot(self.spark, targets["target"]))

    def test_publication_candidate_mutation_is_blocked(self):
        targets, candidate, _ = self.publication_fixture()
        table = candidate["tables"]["mart"]["name"]
        replace_table(
            self.spark,
            self.spark.table(table).withColumn("id", F.lit(2).cast("long")),
            table,
            self.stage,
        )
        with (
            patch.object(pipeline, "TARGET", targets["target"]),
            patch.object(pipeline, "session", return_value=self.spark),
            patch.object(self.spark, "stop"),
        ):
            with self.assertRaisesRegex(ValueError, "Candidate changed"):
                pipeline.publish(self.key)

    def test_publication_changed_parent_is_blocked(self):
        targets, _, _ = self.publication_fixture()
        replace_table(
            self.spark,
            self.spark.createDataFrame([("other",)], "publication_id string"),
            targets["target"],
            self.stage,
        )
        with (
            patch.object(pipeline, "TARGET", targets["target"]),
            patch.object(pipeline, "session", return_value=self.spark),
            patch.object(self.spark, "stop"),
        ):
            with self.assertRaisesRegex(ValueError, "parent publication changed"):
                pipeline.publish(self.key)

    def test_unrelated_reference_change_cannot_be_overwritten(self):
        targets, _, _ = self.publication_fixture()
        replace_table(
            self.spark,
            self.spark.createDataFrame([("external", 9)], "publication_id string,id long"),
            targets["facts_target"],
            self.stage,
        )
        with (
            patch.object(pipeline, "TARGET", targets["target"]),
            patch.object(pipeline, "FACT_TARGET", targets["facts_target"]),
            patch.object(pipeline, "SILVER_TARGET", targets["silver_target"]),
            patch.object(pipeline, "session", return_value=self.spark),
            patch.object(self.spark, "stop"),
        ):
            with self.assertRaisesRegex(ValueError, "Reference input changed"):
                pipeline.publish(self.key)

    def test_full_pipeline_replacement_removes_old_lines_and_preserves_direct(self):
        paths = ProjectPaths(ProjectPaths.from_env().project_root, self.stage / "sandbox")
        targets = {
            name: self.table(name.lower())
            for name in (
                "TARGET",
                "FACT_TARGET",
                "SILVER_TARGET",
                "ORDER_TARGET",
                "PRODUCT_TARGET",
                "DIRECT_MART_TARGET",
                "BRONZE_TARGET",
                "CHANNEL_TARGET",
                "DATE_TARGET",
            )
        }
        orders = self.spark.createDataFrame(
            [
                (
                    "d1",
                    date(2026, 6, 1),
                    "web",
                    Decimal("50"),
                    Decimal("65"),
                    True,
                    "postgres/shopvn",
                    "source",
                    datetime(2026, 11, 12, tzinfo=timezone.utc),
                )
            ],
            "source_order_id string,order_date date,channel string,merchandise_amount decimal(20,2),net_customer_amount decimal(20,2),is_revenue_eligible boolean,source_system string,data_origin string,observed_at timestamp",
        )
        lines = self.spark.createDataFrame(
            [("d-key", "d1", 20260601, 1, 1, True, "postgres/shopvn", "source")],
            "sales_line_key string,source_order_id string,order_date_key int,channel_key long,quantity long,is_sales_eligible boolean,source_system string,data_origin string",
        )
        product = self.spark.createDataFrame(
            [(1, 1, datetime(2026, 11, 11, tzinfo=timezone.utc), None, True)],
            "product_id int,product_key long,valid_from timestamp,valid_to timestamp,is_current boolean",
        )
        direct_mart = self.spark.createDataFrame(
            [(date(2026, 6, 1), "web", 1, Decimal("65"))],
            "order_date date,channel string,eligible_order_count long,net_customer_amount decimal(20,2)",
        )
        for name, frame in [
            ("ORDER_TARGET", orders),
            ("FACT_TARGET", lines),
            ("PRODUCT_TARGET", product),
            ("DIRECT_MART_TARGET", direct_mart),
        ]:
            replace_table(self.spark, frame, targets[name], self.stage)

        def capture(ident, prices, clock, state="verified"):
            root = paths.runtime / "marketplace/landing" / ident
            root.mkdir(parents=True)
            path = root / "lazada_20260601.csv"
            with path.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(COLUMNS)
                for order, price in prices:
                    writer.writerow(
                        [
                            order,
                            1,
                            "SKU",
                            "lazada",
                            "2026-06-01",
                            1,
                            price,
                            0,
                            "0.08",
                            price * 92 // 100,
                            "2026-06-16",
                            "completed",
                        ]
                    )
            item = {
                "partner": "lazada",
                "business_date": "2026-06-01",
                "filename": path.name,
                "capture_id": ident,
                "state": state,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "reference": {
                    "accepted": len(prices),
                    "units": len(prices),
                    "merchandise": str(sum(p for _, p in prices)),
                    "reported_net": str(sum(p * 92 // 100 for _, p in prices)),
                    "eligible_orders": len(prices),
                },
            }
            manifest = {
                "capture_id": ident,
                "contract_version": 1,
                "rule_version": "1.1",
                "data_origin": "source",
                "start": "2026-06-01",
                "end": "2026-06-01",
                "observed_at": clock,
                "files": [item],
                "states": {state: 1},
            }
            (root / "manifest.json").write_text(json.dumps(manifest))

        def approve(key):
            root = pipeline.folder(key)
            payload = json.loads((root / "candidate.json").read_text())
            (root / "validation.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "candidate_sha256": hashlib.sha256(
                            json.dumps(payload, sort_keys=True).encode()
                        ).hexdigest(),
                    }
                )
            )

        with ExitStack() as stack:
            stack.enter_context(patch.multiple(pipeline, **targets))
            stack.enter_context(
                patch.object(
                    pipeline,
                    "table_names",
                    side_effect=lambda k: {
                        n: f"shopvn.stage2_tests.{n}_{k}"
                        for n in ("registry", "silver", "facts", "mart")
                    },
                )
            )
            stack.enter_context(patch.object(ProjectPaths, "from_env", return_value=paths))
            stack.enter_context(patch.object(pipeline, "session", return_value=self.spark))
            stack.enter_context(patch.object(self.spark, "stop"))
            for number, prices, state, wanted in [
                (1, [("o1", 100), ("o2", 200)], "verified", 276),
                (2, [], "checksum_mismatch", 276),
                (3, [("o2", 200)], "verified", 184),
                (4, [], "verified", 0),
            ]:
                ident = uuid.uuid4().hex
                key = uuid.uuid4().hex[:24]
                capture(ident, prices, f"2026-11-12T0{number}:00:00Z", state)
                pipeline.prepare(ident, key)
                approve(
                    key
                )  # Synthetic approval only; actual Trino gate is tested by the real DAG.
                pipeline.publish(key)
                m = (
                    self.spark.table(targets["TARGET"])
                    .filter("channel='lazada' AND report_type='sales'")
                    .first()
                )
                self.assertEqual(m.partner_reported_net, wanted)
                self.assertEqual(
                    self.spark.table(targets["FACT_TARGET"])
                    .filter("source_system='postgres/shopvn'")
                    .count(),
                    1,
                )
                if number == 2:
                    self.assertEqual(m.coverage_status, "retained_unverified")
                if number == 3:
                    kept = (
                        self.spark.table(targets["FACT_TARGET"])
                        .filter("source_system LIKE 'marketplace/%'")
                        .collect()
                    )
                    self.assertEqual(len(kept), 1)
                    self.assertEqual(kept[0].source_order_id, "o2")
                if number == 4:
                    self.assertIsNone(m.aov_merchandise)
            remaining = self.spark.table(targets["FACT_TARGET"]).filter(
                "source_system LIKE 'marketplace/%'"
            )
            self.assertEqual(remaining.count(), 0)
            self.assertEqual(self.spark.table(targets["BRONZE_TARGET"]).count(), 3)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(MarketplaceTests)
    )
    output = ProjectPaths.from_env().runtime / "stage2/spark-tests.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "status": "passed" if result.wasSuccessful() else "failed",
                "tests": result.testsRun,
                "errors": len(result.errors),
                "failures": len(result.failures),
                "scope": "Synthetic tables only in shopvn.stage2_tests; no source mutations",
            },
            indent=2,
        )
    )
    raise SystemExit(0 if result.wasSuccessful() else 1)
