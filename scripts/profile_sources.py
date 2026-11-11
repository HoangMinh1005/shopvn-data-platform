"""Read-only profiling; export aggregate/schema metadata, never customer records."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path

import paramiko
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from shopvn.common.environment import load_env

ROOT = Path(__file__).resolve().parents[1]
TABLES = (
    "customers", "products", "orders", "order_items", "vouchers", "inventory",
    "inventory_transactions", "returns", "product_reviews",
)


def profile_postgres() -> tuple[dict, list[str]]:
    result: dict = {}
    with psycopg.connect(
        host=os.getenv("SHOPVN_DB_HOST", "127.0.0.1"),
        port=os.getenv("SHOPVN_DB_HOST_PORT", "5434"),
        dbname=os.getenv("SHOPVN_DB_NAME", "shopvn"),
        user=os.environ["SHOPVN_DB_READER_USER"],
        password=os.environ["SHOPVN_DB_READER_PASSWORD"],
        options="-c default_transaction_read_only=on -c statement_timeout=120000",
        row_factory=dict_row, autocommit=True,
    ) as conn:
        def query(statement: str, params=None) -> list[dict]:
            return conn.execute(statement, params).fetchall()

        result["session"] = query(
            "SELECT current_user, current_setting('TimeZone') AS server_timezone, "
            "current_setting('default_transaction_read_only') AS read_only, "
            "version() AS server_version"
        )
        result["privileges"] = query(
            "SELECT table_name, privilege_type FROM information_schema.role_table_grants "
            "WHERE grantee=current_user AND table_schema='public' ORDER BY 1,2"
        )
        result["schema"] = query(
            "SELECT table_name,column_name,data_type,is_nullable,numeric_precision,"
            "numeric_scale FROM information_schema.columns WHERE table_schema='public' "
            "ORDER BY table_name,ordinal_position"
        )
        result["constraints"] = query(
            "SELECT tc.table_name,tc.constraint_type,kcu.column_name,"
            "ccu.table_name AS referenced_table,ccu.column_name AS referenced_column "
            "FROM information_schema.table_constraints tc "
            "LEFT JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_name=kcu.constraint_name AND tc.constraint_schema=kcu.constraint_schema "
            "LEFT JOIN information_schema.constraint_column_usage ccu "
            "ON tc.constraint_name=ccu.constraint_name AND tc.constraint_schema=ccu.constraint_schema "
            "WHERE tc.table_schema='public' AND tc.constraint_type IN ('PRIMARY KEY','FOREIGN KEY','UNIQUE') "
            "ORDER BY tc.table_name,tc.constraint_type,kcu.column_name"
        )
        result["row_counts"] = {}
        result["null_counts"] = {}
        for table in TABLES:
            result["row_counts"][table] = conn.execute(
                sql.SQL("SELECT COUNT(*) AS n FROM {} ").format(sql.Identifier(table))
            ).fetchone()["n"]
            columns = [r["column_name"] for r in result["schema"] if r["table_name"] == table]
            expressions = [sql.SQL("COUNT(*) FILTER (WHERE {} IS NULL) AS {}").format(
                sql.Identifier(column), sql.Identifier(column)) for column in columns]
            result["null_counts"][table] = conn.execute(
                sql.SQL("SELECT {} FROM {}").format(sql.SQL(",").join(expressions), sql.Identifier(table))
            ).fetchone()

        checks = {
            "order_status_payment": "SELECT status,payment_status,COUNT(*) AS n FROM orders GROUP BY 1,2 ORDER BY 1,2",
            "order_channels": "SELECT channel,COUNT(*) AS n FROM orders GROUP BY 1 ORDER BY 1",
            "date_range": "SELECT MIN(order_date) AS min_date,MAX(order_date) AS max_date,MIN(created_at) AS min_created,MAX(created_at) AS max_created FROM orders",
            "line_reconciliation": "WITH l AS (SELECT order_id,SUM(total_price) AS amount FROM order_items GROUP BY 1) SELECT COUNT(*) FILTER (WHERE l.order_id IS NULL) AS orders_without_lines, COUNT(*) FILTER (WHERE o.subtotal<>l.amount) AS subtotal_mismatches, COALESCE(SUM(o.subtotal-l.amount),0) AS difference FROM orders o LEFT JOIN l USING(order_id)",
            "amount_anomalies": "SELECT COUNT(*) FILTER (WHERE discount_amount>subtotal) AS excessive_discount,COUNT(*) FILTER (WHERE shipping_fee=0) AS zero_shipping,COUNT(*) FILTER (WHERE total_amount<>GREATEST(0,subtotal+shipping_fee-discount_amount)) AS total_formula_mismatches,COUNT(*) FILTER (WHERE discount_amount<0 OR subtotal<0 OR shipping_fee<0) AS negative_input FROM orders",
            "line_anomalies": "SELECT COUNT(*) FILTER (WHERE quantity=0) AS zero_quantity,COUNT(*) FILTER (WHERE quantity<0) AS negative_quantity,COUNT(*) FILTER (WHERE total_price<>(unit_price-discount_per_item)*quantity) AS line_formula_mismatches FROM order_items",
            "product_anomalies": "SELECT COUNT(*) FILTER (WHERE cost_price>base_price) AS cost_above_base FROM products",
            "orphan_keys": "SELECT (SELECT COUNT(*) FROM orders o LEFT JOIN customers c USING(customer_id) WHERE c.customer_id IS NULL) AS orders_customers,(SELECT COUNT(*) FROM order_items i LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL) AS lines_orders,(SELECT COUNT(*) FROM order_items i LEFT JOIN products p USING(product_id) WHERE p.product_id IS NULL) AS lines_products,(SELECT COUNT(*) FROM returns r LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL) AS returns_orders",
            "return_status": "SELECT status,COUNT(*) AS n,SUM(refund_amount) AS amount FROM returns GROUP BY 1 ORDER BY 1",
            "multiple_returns": "SELECT COUNT(*) FILTER (WHERE n>1) AS orders_with_multiple_returns,COALESCE(MAX(n),0) AS max_returns_per_order FROM (SELECT order_id,COUNT(*) n FROM returns GROUP BY 1) t",
            "refund_bounds": "WITH r AS (SELECT order_id,SUM(refund_amount) FILTER (WHERE status='refunded') AS refunded FROM returns GROUP BY 1) SELECT COUNT(*) FILTER (WHERE r.refunded>o.total_amount) AS above_customer_total,COUNT(*) FILTER (WHERE r.refunded>o.subtotal-o.discount_amount AND o.discount_amount<=o.subtotal) AS above_merchandise,COUNT(*) FILTER (WHERE r.refunded=o.total_amount AND o.shipping_fee>0) AS equals_total_with_shipping,COUNT(*) FILTER (WHERE r.refunded<0) AS negative_refunds FROM orders o JOIN r USING(order_id)",
            "refund_by_order_status": "SELECT o.status,o.payment_status,r.status AS return_status,COUNT(*) AS n,SUM(r.refund_amount) AS amount FROM returns r JOIN orders o USING(order_id) GROUP BY 1,2,3 ORDER BY 1,2,3",
            "inventory_types": "SELECT type,COUNT(*) AS n,SUM(qty_change) AS total_change,MIN(qty_change) AS min_change,MAX(qty_change) AS max_change FROM inventory_transactions GROUP BY 1 ORDER BY 1",
            "inventory_anchor": "SELECT COUNT(*) AS products,MIN(last_updated) AS min_anchor,MAX(last_updated) AS max_anchor,COUNT(DISTINCT warehouse_id) AS warehouses,COUNT(*) FILTER (WHERE stock_qty<0) AS negative_stock FROM inventory",
            "inventory_after_anchor": "SELECT COUNT(*) AS transactions_after_product_anchor FROM inventory_transactions t JOIN inventory i USING(product_id) WHERE t.created_at>i.last_updated",
            "inventory_txn_range": "SELECT MIN(created_at) AS first_transaction,MAX(created_at) AS last_transaction FROM inventory_transactions",
            "review_scope": "SELECT o.channel,COUNT(*) AS n,MIN(rating) AS min_rating,MAX(rating) AS max_rating FROM product_reviews r JOIN orders o USING(order_id) GROUP BY 1 ORDER BY 1",
            "direct_revenue_baseline": "WITH r AS (SELECT order_id,COALESCE(SUM(refund_amount) FILTER (WHERE status='refunded'),0) AS refund FROM returns GROUP BY 1) SELECT o.channel,COUNT(*) AS eligible_orders,SUM(o.subtotal-o.discount_amount) AS merchandise,SUM(COALESCE(r.refund,0)) AS refunded,SUM(o.subtotal-o.discount_amount-COALESCE(r.refund,0)) AS adjusted_merchandise,SUM(o.total_amount-COALESCE(r.refund,0)) AS adjusted_customer_total FROM orders o LEFT JOIN r USING(order_id) WHERE o.status IN ('delivered','returned') AND o.payment_status IN ('paid','refunded') AND o.discount_amount<=o.subtotal GROUP BY 1 ORDER BY 1",
        }
        for name, statement in checks.items():
            result[name] = query(statement)
        api_ids = [r["order_id"] for r in query(
            "SELECT order_id FROM (SELECT order_id,status,ROW_NUMBER() OVER(PARTITION BY status ORDER BY order_id) AS rn FROM orders) t WHERE rn<=10 ORDER BY status,order_id"
        )]
        result["product_ids_for_sftp"] = [r["product_id"] for r in query("SELECT product_id FROM products")]
    return result, api_ids


def profile_sftp(product_ids: set[int]) -> dict:
    host = os.getenv("SHOPVN_SFTP_HOST", "127.0.0.1")
    if host not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Demo profiler only trusts loopback SFTP; pin host key before remote use")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Loopback course image only.
    client.connect(host, int(os.getenv("SHOPVN_SFTP_HOST_PORT", "2222")),
                   username=os.environ["SHOPVN_SFTP_USER"], password=os.environ["SHOPVN_SFTP_PASSWORD"],
                   look_for_keys=False, allow_agent=False, timeout=15)
    result: dict = {"files": [], "status_counts": Counter(), "checks": Counter(), "net_delta_vnd": Counter()}
    candidates: dict[str, Counter] = {"partner_order_product": Counter(), "partner_order_product_sku": Counter()}
    totals: dict[str, Decimal] = Counter()
    fingerprint = hashlib.sha256(client.get_transport().get_remote_server_key().asbytes()).hexdigest()
    result["loopback_host_key_sha256_hex"] = fingerprint
    with client, client.open_sftp() as sftp:
        for partner in ("lazada", "shopee", "tiktok"):
            directory = f"/marketplace/incoming/{partner}"
            available = set(sftp.listdir(directory))
            for offset in range(30):
                day = date(2026, 6, 1) + timedelta(days=offset)
                filename = f"{partner}_{day:%Y%m%d}.csv"
                record = {"partner": partner, "date": day.isoformat(), "filename": filename}
                if filename not in available:
                    record["state"] = "missing"
                    result["files"].append(record)
                    continue
                if filename + ".md5" not in available:
                    record["state"] = "checksum_missing"
                    result["files"].append(record)
                    continue
                # A local ignored spool bounds RAM and allows checksum validation before parsing.
                spool_dir = ROOT / "runtime" / "profiling" / "sftp"
                spool_dir.mkdir(parents=True, exist_ok=True)
                spool = spool_dir / filename
                digest = hashlib.md5()
                with sftp.open(f"{directory}/{filename}", "rb") as source, spool.open("wb") as target:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                        target.write(chunk)
                with sftp.open(f"{directory}/{filename}.md5", "r") as checksum:
                    expected = checksum.read().decode().strip().split()[0]
                record["bytes"] = spool.stat().st_size
                if digest.hexdigest().lower() != expected.lower():
                    record["state"] = "checksum_mismatch"
                    result["files"].append(record)
                    continue
                record.update(state="valid", rows=0)
                with spool.open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    result.setdefault("columns", reader.fieldnames)
                    for row in reader:
                        record["rows"] += 1
                        result["status_counts"][row["status"]] += 1
                        key = (partner, row["external_order_id"], row["shopvn_product_id"])
                        candidates["partner_order_product"][key] += 1
                        candidates["partner_order_product_sku"][key + (row["seller_sku"],)] += 1
                        price = Decimal(row["sale_price"])
                        quantity = int(row["quantity_sold"])
                        net = Decimal(row["net_revenue"])
                        calculated = (price * quantity * (1 - Decimal(row["commission_rate"]))).to_integral_value(rounding=ROUND_FLOOR)
                        result["checks"]["net_formula_mismatches"] += int(net != calculated)
                        float_calculated = int(float(row["sale_price"]) * quantity * (1 - float(row["commission_rate"])))
                        result["checks"]["float_source_formula_mismatches"] += int(net != float_calculated)
                        result["net_delta_vnd"][str(net - calculated)] += 1
                        result["checks"]["unknown_product"] += int(int(row["shopvn_product_id"]) not in product_ids)
                        result["checks"]["nonpositive_quantity"] += int(quantity <= 0)
                        result["checks"]["partner_mismatch"] += int(row["partner"] != partner)
                        result["checks"]["settlement_not_t15"] += int(date.fromisoformat(row["settlement_date"]) != date.fromisoformat(row["order_date"]) + timedelta(days=15))
                        if row["status"] == "completed":
                            totals[f"{partner}_merchandise"] += price * quantity
                            totals[f"{partner}_reported_net"] += net
                result["files"].append(record)
    result["candidate_keys"] = {name: {"distinct": len(counts), "duplicate_groups": sum(n > 1 for n in counts.values()), "duplicate_excess": sum(n - 1 for n in counts.values())} for name, counts in candidates.items()}
    result["completed_totals"] = totals
    result["file_states"] = Counter(r["state"] for r in result["files"])
    return result


def profile_api(order_ids: list[str]) -> dict:
    base = os.getenv("SHOPVN_LOGISTICS_API_URL", "http://127.0.0.1:8000")
    result: dict = {"sample_requested": len(order_ids), "http_attempts": Counter(), "shipment_status": Counter(), "fields": [], "returned": 0, "not_found": 0, "failed_requests": 0}
    for start in range(0, len(order_ids), 50):
        ids = order_ids[start:start + 50]
        request = urllib.request.Request(base + "/v1/shipments?order_ids=" + ",".join(ids), headers={"X-API-Key": os.environ["SHOPVN_LOGISTICS_API_KEY"]})
        payload = None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    result["http_attempts"][str(response.status)] += 1
                    payload = json.load(response)
                    break
            except urllib.error.HTTPError as exc:
                result["http_attempts"][str(exc.code)] += 1
                if exc.code == 404:
                    result["not_found"] += len(ids)
                    break
                if exc.code not in {429, 500} or attempt == 3:
                    result["failed_requests"] += 1
                    break
                time.sleep(min(int(exc.headers.get("Retry-After", 2 ** attempt)), 30))
            except (TimeoutError, urllib.error.URLError):
                result["http_attempts"]["timeout_or_transport"] += 1
                if attempt == 3:
                    result["failed_requests"] += 1
                else:
                    time.sleep(2 ** attempt)
        if payload is None:
            continue
        rows = payload if isinstance(payload, list) else payload.get("shipments", payload.get("data", []))
        result["response_fields"] = sorted(payload) if isinstance(payload, dict) else ["list"]
        if isinstance(rows, dict):
            rows = list(rows.values())
        result["returned"] += len(rows)
        result["not_found"] += max(0, len(ids) - len(rows))
        for row in rows:
            result["shipment_status"][row.get("status", "unknown")] += 1
            result["fields"] = sorted(set(result["fields"]) | set(row))
    result["scope"] = "Stratified first 10 orders per order status; not full API reconciliation"
    # Compare response stability without exporting the order ID or shipment payload.
    if order_ids:
        single = urllib.request.Request(base + "/v1/shipments/" + order_ids[-1],
            headers={"X-API-Key": os.environ["SHOPVN_LOGISTICS_API_KEY"]})
        samples = []
        for _ in range(2):
            try:
                with urllib.request.urlopen(single, timeout=15) as response:
                    samples.append(json.load(response))
            except (urllib.error.URLError, TimeoutError):
                pass
        result["repeated_single_successes"] = len(samples)
        result["same_order_payload_stable"] = samples[0] == samples[1] if len(samples) == 2 else None
        result["same_order_tracking_stable"] = samples[0].get("tracking_code") == samples[1].get("tracking_code") if len(samples) == 2 else None
    return result


def serialize(value):
    if isinstance(value, (Decimal, date, datetime)):
        return str(value)
    raise TypeError(type(value).__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/profiling/source-profile.json")
    args = parser.parse_args()
    load_env()
    pg, order_ids = profile_postgres()
    product_ids = set(pg.pop("product_ids_for_sftp"))
    print("PostgreSQL aggregate profiling complete", flush=True)
    sftp = profile_sftp(product_ids)
    print("SFTP checksum/stream profiling complete", flush=True)
    api = profile_api(order_ids)
    result = {"observed_at_utc": datetime.now(timezone.utc).isoformat(), "postgres": pg, "sftp": sftp, "api": api}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, default=serialize, indent=2, ensure_ascii=False), encoding="utf-8")
    schema_output = ROOT / "schemas/sources/postgres-observed.json"
    schema_output.write_text(json.dumps({"observed_at_utc": result["observed_at_utc"],
        "columns": pg["schema"], "constraints": pg["constraints"]}, default=serialize, indent=2), encoding="utf-8")
    print(f"Aggregate report: {args.output}")


if __name__ == "__main__":
    main()
