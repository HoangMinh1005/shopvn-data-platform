"""Extract a consistent, read-only direct-sales snapshot; never export direct customer PII."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = {
    "orders": "order_id,customer_id,voucher_code,channel,order_date,status,subtotal,shipping_fee,discount_amount,total_amount,payment_method,payment_status,created_at",
    "order_items": "item_id,order_id,product_id,quantity,unit_price,discount_per_item,total_price",
    "returns": "return_id,order_id,reason,status,refund_amount,created_at",
    "customers": "customer_id,loyalty_tier,gender,city",
    "products": "product_id,sku,name,category,subcategory,brand,base_price,cost_price,weight_gram,is_active",
    "vouchers": "voucher_code,type,value,min_order_value,max_discount,campaign_id,valid_from,valid_to,max_usage",
}


def load_env():
    if not (ROOT / ".env").exists():
        return  # Containers receive credentials through environment, never a mounted .env.
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip())


def extract() -> Path:
    load_env()
    snapshot_id = uuid.uuid4().hex
    folder = ROOT / "runtime/direct/landing" / snapshot_id
    folder.mkdir(parents=True)
    key_path = ROOT / "runtime/direct/hmac.key"
    if not key_path.exists():
        with key_path.open("xb") as stream:
            stream.write(secrets.token_bytes(32))
    report = {
        "snapshot_id": snapshot_id,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "rule_version": "1.1",
        "tables": {},
        "contains_contact_pii_fields": False,
    }
    with psycopg.connect(
        host=os.getenv("SHOPVN_DB_HOST", "127.0.0.1"),
        port=os.getenv("SHOPVN_DB_HOST_PORT", "5434"),
        dbname=os.getenv("SHOPVN_DB_NAME", "shopvn"),
        user=os.environ["SHOPVN_DB_READER_USER"],
        password=os.environ["SHOPVN_DB_READER_PASSWORD"],
        row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=120000",
    ) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        report["source_transaction"] = conn.execute(
            "SELECT current_setting('transaction_isolation') AS isolation, "
            "current_setting('transaction_read_only') AS read_only, "
            "pg_current_snapshot()::text AS snapshot"
        ).fetchone()
        for table, columns in EXPORTS.items():
            destination = folder / f"{table}.csv"
            statement = sql.SQL(
                "COPY (SELECT {} FROM {}) TO STDOUT WITH (FORMAT CSV, HEADER true)"
            ).format(
                sql.SQL(",").join(sql.Identifier(column) for column in columns.split(",")),
                sql.Identifier(table),
            )
            with conn.cursor().copy(statement) as copy, destination.open("wb") as output:
                for block in copy:
                    output.write(block)
            with destination.open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            count = conn.execute(
                sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))
            ).fetchone()["n"]
            report["tables"][table] = {
                "rows": count,
                "sha256": checksum,
                "bytes": destination.stat().st_size,
            }
        statement = (ROOT / "sql/reconciliation/direct_revenue_baseline.sql").read_text(
            encoding="utf-8"
        )
        report["fin02_expected_by_day_channel"] = conn.execute(statement).fetchall()
        report["source_order_date_range"] = conn.execute(
            "SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date FROM orders"
        ).fetchone()
    report["extraction_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    manifest = folder / "manifest.json"
    manifest.write_text(
        json.dumps(report, default=str, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (ROOT / "runtime/direct/latest-snapshot.json").write_text(
        json.dumps({"snapshot_id": snapshot_id}, indent=2), encoding="utf-8"
    )
    print(
        f"Read-only snapshot {snapshot_id}: "
        + ", ".join(f"{k}={v['rows']}" for k, v in report["tables"].items()),
        flush=True,
    )
    return manifest


if __name__ == "__main__":
    extract()
