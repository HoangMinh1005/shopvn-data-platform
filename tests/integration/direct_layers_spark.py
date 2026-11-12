"""Run with spark-submit: synthetic regression tests exercise the actual layer functions."""

import json
import sys
import unittest
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shopvn.config import ProjectPaths
from shopvn.contracts.direct import SCHEMAS
from shopvn.marts.finance import build_fin02
from shopvn.quality.direct import validate_allocated_lines, validate_silver
from shopvn.transformations.gold.orders import build_order_fact, enrich_orders
from shopvn.transformations.gold.returns import build_return_fact
from shopvn.transformations.gold.sales_lines import build_sales_line_fact
from shopvn.transformations.silver.direct import normalize


class DirectLayerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("shopvn-layer-regression")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, table, rows):
        raw = self.spark.sparkContext.parallelize([json.dumps(row) for row in rows])
        return (
            self.spark.read.schema(SCHEMAS[table])
            .json(raw)
            .withColumn("source_record_hash", F.lit("hash"))
            .withColumn("snapshot_id", F.lit("fixture"))
        )

    def inputs(self):
        orders = self.frame(
            "orders",
            [
                {
                    "order_id": "A",
                    "customer_id": 1,
                    "channel": "web",
                    "order_date": "2026-06-30",
                    "status": "delivered",
                    "payment_status": "paid",
                    "subtotal": 200,
                    "discount_amount": 10.5,
                    "shipping_fee": 5,
                    "total_amount": 194.5,
                }
            ],
        )
        lines = self.frame(
            "order_items",
            [
                {
                    "item_id": 1,
                    "order_id": "A",
                    "product_id": 1,
                    "quantity": 1,
                    "unit_price": 100,
                    "discount_per_item": 0,
                    "total_price": 100,
                },
                {
                    "item_id": 2,
                    "order_id": "A",
                    "product_id": 1,
                    "quantity": 1,
                    "unit_price": 100,
                    "discount_per_item": 0,
                    "total_price": 100,
                },
                {
                    "item_id": 3,
                    "order_id": "A",
                    "product_id": 1,
                    "quantity": 0,
                    "unit_price": 100,
                    "discount_per_item": 0,
                    "total_price": 0,
                },
            ],
        )
        returns = self.frame(
            "returns",
            [
                {"return_id": 1, "order_id": "A", "status": "refunded", "refund_amount": 20},
                {"return_id": 2, "order_id": "A", "status": "requested", "refund_amount": 7},
            ],
        )
        ref = F.udf(lambda value: "customer-1", "string")
        customers = self.spark.createDataFrame(
            [("customer-1", 1)], "customer_ref string,customer_key long"
        )
        products = self.spark.createDataFrame([(1, 1)], "product_id int,product_key long")
        customers = customers.withColumn("order_date", F.to_date(F.lit("2026-06-30"))).withColumn(
            "customer_dimension_match_basis", F.lit("initial_snapshot_fallback")
        )
        products = products.withColumn("order_date", F.to_date(F.lit("2026-06-30"))).withColumn(
            "product_dimension_match_basis", F.lit("initial_snapshot_fallback")
        )
        enriched = enrich_orders(orders, returns, ref, customers)
        return orders, lines, returns, enriched, products

    def test_gold_and_fin02_preserve_refund_and_fractional_allocation(self):
        orders, lines, returns, enriched, products = self.inputs()
        validate_silver({"orders": orders, "order_items": lines})
        order_fact = build_order_fact(enriched)
        line_fact = build_sales_line_fact(lines, enriched, products)
        validate_allocated_lines(order_fact, line_fact)
        allocations = {
            row.source_line_id: row.allocated_header_discount_amount for row in line_fact.collect()
        }
        self.assertEqual(
            allocations, {"1": Decimal("5.50"), "2": Decimal("5.00"), "3": Decimal("0.00")}
        )
        self.assertEqual(build_fin02(order_fact).first().net_customer_amount, Decimal("174.50"))
        self.assertEqual(
            build_return_fact(returns, enriched).agg(F.sum("refunded_order_amount")).first()[0],
            Decimal("20.00"),
        )

    def test_silver_normalizes_postgres_boolean(self):
        source = self.spark.createDataFrame([("t",), ("f",)], "is_active string")
        self.assertEqual(
            [row.is_active for row in normalize("products", source).collect()], [True, False]
        )

    def test_silver_rejects_unknown_boolean(self):
        with self.assertRaisesRegex(ValueError, "Unknown PostgreSQL boolean"):
            normalize("products", self.spark.createDataFrame([("maybe",)], "is_active string"))

    def test_silver_blocks_source_total_mismatch(self):
        orders, lines, _, _, _ = self.inputs()
        with self.assertRaisesRegex(ValueError, "Source total mismatch"):
            validate_silver(
                {"orders": orders.withColumn("total_amount", F.lit(0)), "order_items": lines}
            )


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(DirectLayerTests)
    )
    output = ProjectPaths.from_env().runtime / "refactor"
    output.mkdir(parents=True, exist_ok=True)
    (output / "spark-layer-tests.json").write_text(
        json.dumps(
            {
                "status": "passed" if result.wasSuccessful() else "failed",
                "tests": result.testsRun,
                "failures": len(result.failures),
                "errors": len(result.errors),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    sys.exit(0 if result.wasSuccessful() else 1)
