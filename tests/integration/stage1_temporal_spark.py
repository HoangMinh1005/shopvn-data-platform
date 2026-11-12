"""Synthetic temporal/persistence tests, isolated from actual tables in stage1_tests."""

import json
import sys
import unittest
import uuid
from datetime import date

from pyspark.sql import functions as F

from shopvn.common.spark_runtime import session
from shopvn.config import ProjectPaths
from shopvn.contracts.direct import SCHEMAS
from shopvn.contracts.inventory import InventoryPolicy
from shopvn.quality.direct import validate_silver
from shopvn.quality.temporal import validate_history
from shopvn.transformations.gold.inventory import persist_policy, publish_stock
from shopvn.transformations.gold.orders import build_order_fact, enrich_orders
from shopvn.transformations.gold.sales_lines import build_sales_line_fact
from shopvn.transformations.gold.temporal import lookup_day_versions, observed_dimension
from shopvn.transformations.silver.direct import normalize

T1 = "2026-11-11T15:00:00+00:00"
T2 = "2026-11-11T17:00:00+00:00"
T3 = "2026-11-12T02:00:00+00:00"


class Stage1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session("shopvn-stage1-temporal-regressions")
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS shopvn.stage1_tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.identity = uuid.uuid4().hex[:12]
        self.stage = ProjectPaths.from_env().runtime / "stage1/tests" / self.identity
        self.table = "shopvn.stage1_tests.dim_" + self.identity

    def observe(self, tier, clock):
        incoming = self.spark.createDataFrame(
            [("c1", tier)], "customer_ref string,loyalty_tier string"
        )
        return observed_dimension(
            self.spark, incoming, self.table, "customer_key", "customer_ref", clock, self.stage
        )

    def test_scd_changes_replay_and_day_asof(self):
        first = self.observe("Bronze", T1)
        key = first.filter("customer_key<>0").first().customer_key
        self.assertEqual(first.count(), 2)
        self.assertEqual(self.observe("Bronze", T1).count(), 2)
        self.observe("Gold", T2)
        history = self.observe("Gold", T3)
        self.assertEqual(history.count(), 3)
        current = history.filter("customer_key<>0 AND is_current").first()
        self.assertNotEqual(current.customer_key, key)
        closed = history.filter(F.col("customer_key") == key).first()
        self.assertEqual(closed.valid_to, current.valid_from)
        requests = self.spark.createDataFrame(
            [("c1", date(2026, 6, 30)), ("c1", date(2026, 11, 11)), ("c1", date(2026, 11, 12))],
            "customer_ref string,order_date date",
        )
        mapped = {
            str(row.order_date): (row.customer_key, row.dimension_match_basis)
            for row in lookup_day_versions(
                requests, history, "customer_ref", "customer_key", T3
            ).collect()
        }
        self.assertEqual(mapped["2026-06-30"], (key, "initial_snapshot_fallback"))
        self.assertEqual(mapped["2026-11-11"], (key, "observed_day_asof"))
        self.assertEqual(mapped["2026-11-12"], (current.customer_key, "observed_day_asof"))
        with self.assertRaisesRegex(ValueError, "Stale observation"):
            self.observe("Bronze", T1)

    def test_asof_does_not_see_later_same_day_versions(self):
        self.observe("Bronze", "2026-11-11T01:00:00Z")
        second = self.observe("Silver", "2026-11-11T02:00:00Z")
        silver_key = second.filter("is_current AND customer_key<>0").first().customer_key
        third = self.observe("Gold", "2026-11-11T03:00:00Z")
        requests = self.spark.createDataFrame(
            [("c1", date(2026, 11, 11))], "customer_ref string,order_date date"
        )
        result = lookup_day_versions(
            requests, third, "customer_ref", "customer_key", "2026-11-11T02:00:00Z"
        ).first()
        self.assertEqual(result.customer_key, silver_key)

    def test_same_observation_changed_payload_rejected(self):
        self.observe("Bronze", T1)
        with self.assertRaisesRegex(ValueError, "Conflicting payload"):
            self.observe("Gold", T1)
        self.assertEqual(
            self.spark.table(self.table).filter("customer_key<>0").first().loyalty_tier, "Bronze"
        )

    def test_same_day_before_first_observation_is_labelled_fallback(self):
        history = self.observe("Bronze", T1)
        requests = self.spark.createDataFrame(
            [("c1", date(2026, 11, 11))], "customer_ref string,order_date date"
        )
        row = lookup_day_versions(
            requests, history, "customer_ref", "customer_key", "2026-11-11T14:00:00Z"
        ).first()
        self.assertEqual(row.dimension_match_basis, "initial_snapshot_fallback")

    def test_undated_dimension_lookup_is_rejected(self):
        lookup = self.spark.createDataFrame([("c1", 1)], "customer_ref string,customer_key long")
        with self.assertRaisesRegex(ValueError, "requires business date"):
            enrich_orders(None, None, None, lookup)

    def test_fact_joins_are_date_grain_and_do_not_multiply_orders(self):
        def frame(name, rows):
            return (
                self.spark.read.schema(SCHEMAS[name])
                .json(self.spark.sparkContext.parallelize([json.dumps(row) for row in rows]))
                .withColumn("source_record_hash", F.lit("fixture"))
                .withColumn("snapshot_id", F.lit("fixture"))
            )

        orders = frame(
            "orders",
            [
                {
                    "order_id": identity,
                    "customer_id": 1,
                    "order_date": day,
                    "channel": "web",
                    "status": "delivered",
                    "payment_status": "paid",
                    "subtotal": 100,
                    "discount_amount": 0,
                    "shipping_fee": 0,
                    "total_amount": 100,
                }
                for identity, day in [("A", "2026-11-11"), ("B", "2026-11-12")]
            ],
        )
        lines = frame(
            "order_items",
            [
                {
                    "item_id": i,
                    "order_id": identity,
                    "product_id": 1,
                    "quantity": 1,
                    "unit_price": 100,
                    "discount_per_item": 0,
                    "total_price": 100,
                }
                for i, identity in [(1, "A"), (2, "B")]
            ],
        )
        returns = frame("returns", [])
        customers = self.spark.createDataFrame(
            [
                ("c1", date(2026, 11, 11), 10, "observed_day_asof"),
                ("c1", date(2026, 11, 12), 20, "observed_day_asof"),
            ],
            "customer_ref string,order_date date,customer_key long,customer_dimension_match_basis string",
        )
        products = self.spark.createDataFrame(
            [
                (1, date(2026, 11, 11), 100, "observed_day_asof"),
                (1, date(2026, 11, 12), 200, "observed_day_asof"),
            ],
            "product_id int,order_date date,product_key long,product_dimension_match_basis string",
        )
        enriched = enrich_orders(orders, returns, F.udf(lambda value: "c1", "string"), customers)
        order_fact = build_order_fact(enriched)
        line_fact = build_sales_line_fact(lines, enriched, products)
        self.assertEqual(
            {r.source_order_id: r.customer_key for r in order_fact.collect()}, {"A": 10, "B": 20}
        )
        self.assertEqual(
            {r.source_order_id: (r.customer_key, r.product_key) for r in line_fact.collect()},
            {"A": (10, 100), "B": (20, 200)},
        )

    def test_duplicate_business_identity_is_rejected(self):
        incoming = self.spark.createDataFrame(
            [("c1", "Bronze"), ("c1", "Gold")], "customer_ref string,loyalty_tier string"
        )
        with self.assertRaisesRegex(ValueError, "Duplicate dimension business"):
            observed_dimension(
                self.spark, incoming, self.table, "customer_key", "customer_ref", T1, self.stage
            )

    def test_overlap_is_rejected(self):
        self.observe("Bronze", T1)
        history = self.observe("Gold", T2)
        broken = history.withColumn(
            "valid_to",
            F.when(
                (F.col("customer_key") != 0) & ~F.col("is_current"), F.to_timestamp(F.lit(T3))
            ).otherwise(F.col("valid_to")),
        )
        with self.assertRaisesRegex(ValueError, "Overlapping"):
            validate_history(broken, "customer_ref", "customer_key")

    def test_missing_dimension_is_not_silently_mapped(self):
        history = self.observe("Bronze", T1)
        requests = self.spark.createDataFrame(
            [("missing", date(2026, 11, 11))], "customer_ref string,order_date date"
        )
        with self.assertRaisesRegex(ValueError, "No observed dimension"):
            lookup_day_versions(requests, history, "customer_ref", "customer_key", T3)

    def test_future_order_is_blocked(self):
        orders = self.spark.createDataFrame([(date(2026, 11, 13),)], "order_date date")
        lines = self.spark.createDataFrame([], "quantity int")
        with self.assertRaisesRegex(ValueError, "Future business date"):
            validate_silver({"orders": orders, "order_items": lines}, T3)

    def test_transactions_do_not_invent_business_or_warehouse_history(self):
        source = self.spark.createDataFrame([(1,)], "txn_id int")
        row = normalize("inventory_transactions", source).first()
        self.assertFalse(row.business_time_reliable)
        self.assertIsNone(row.business_effective_at)
        self.assertIsNone(row.warehouse_id)

    def test_invalid_ratings_are_not_silver_accepted(self):
        source = self.spark.createDataFrame([(1,), (5,), (0,), (6,), (None,)], "rating int")
        self.assertEqual(
            [row.rating for row in normalize("product_reviews", source).collect()], [1, 5]
        )

    def stock(self, changed=False):
        frame = self.spark.createDataFrame(
            [(1, "WH1", 101 if changed else 100), (2, "WH1", 0), (3, "WH2", 50)],
            "product_id int,warehouse_id string,stock_qty int",
        )
        return (
            frame.withColumn("observed_at", F.to_timestamp(F.lit("2026-11-11T17:00:10Z")))
            .withColumn("data_origin", F.lit("test"))
            .withColumn("snapshot_id", F.lit("synthetic"))
        )

    def manifest(self, changed=False):
        return {
            "observed_at_utc": "2026-11-11T17:00:10Z",
            "inventory_expected": [
                {"warehouse_id": "WH1", "rows": 2, "stock_qty": 101 if changed else 100},
                {"warehouse_id": "WH2", "rows": 1, "stock_qty": 50},
            ],
        }

    def test_eod_three_replays_and_conflict_preserve_first_capture(self):
        capture = InventoryPolicy(date(2026, 11, 11)).classify(
            "2026-11-12T00:00:10+07:00", mode="eod"
        )
        current = "shopvn.stage1_tests.current_" + self.identity
        daily = "shopvn.stage1_tests.eod_" + self.identity
        for _ in range(3):
            result = publish_stock(
                self.spark,
                self.stock(),
                self.manifest(),
                capture,
                self.stage,
                current=current,
                daily=daily,
                update_calendar=False,
            )
            self.assertTrue(result["eod_published"])
        self.assertEqual(self.spark.table(daily).count(), 3)
        self.assertEqual(self.spark.table(daily).agg(F.sum("stock_qty")).first()[0], 150)
        with self.assertRaisesRegex(ValueError, "Conflicting EOD payload"):
            publish_stock(
                self.spark,
                self.stock(True),
                self.manifest(True),
                capture,
                self.stage,
                current=current,
                daily=daily,
                update_calendar=False,
            )
        self.assertEqual(self.spark.table(daily).agg(F.sum("stock_qty")).first()[0], 150)

    def test_late_capture_cannot_create_daily_stock(self):
        capture = InventoryPolicy(date(2026, 11, 11)).classify(
            "2026-11-12T00:01:00+07:00", mode="eod"
        )
        current = "shopvn.stage1_tests.current_" + self.identity
        daily = "shopvn.stage1_tests.eod_" + self.identity
        result = publish_stock(
            self.spark,
            self.stock(),
            self.manifest(),
            capture,
            self.stage,
            current=current,
            daily=daily,
            update_calendar=False,
        )
        self.assertFalse(result["eod_published"])
        self.assertFalse(self.spark.catalog.tableExists(daily))

    def test_inventory_policy_is_immutable(self):
        table = "shopvn.stage1_tests.policy_" + self.identity
        policy = InventoryPolicy(date(2026, 11, 11))
        persist_policy(self.spark, policy, T1, self.stage, table)
        persist_policy(self.spark, policy, T3, self.stage, table)
        with self.assertRaisesRegex(ValueError, "immutable"):
            persist_policy(self.spark, InventoryPolicy(date(2026, 11, 10)), T3, self.stage, table)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(Stage1Tests)
    )
    output = ProjectPaths.from_env().runtime / "stage1"
    output.mkdir(parents=True, exist_ok=True)
    (output / "temporal-tests.json").write_text(
        json.dumps(
            {
                "status": "passed" if result.wasSuccessful() else "failed",
                "tests": result.testsRun,
                "failures": len(result.failures),
                "errors": len(result.errors),
                "scope": "Synthetic data in isolated shopvn.stage1_tests; not actual inventory history",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    sys.exit(0 if result.wasSuccessful() else 1)
