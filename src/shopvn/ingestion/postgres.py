"""Extract a consistent, read-only direct-sales snapshot; never export direct customer PII."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from shopvn.common.environment import load_env
from shopvn.config import ProjectPaths
from shopvn.contracts.direct import EXPORTS, RULE_VERSION


def extract(paths: ProjectPaths | None = None) -> Path:
    paths = paths or ProjectPaths.from_env()
    load_env(paths.project_root / ".env")
    snapshot_id = uuid.uuid4().hex
    folder = paths.landing / snapshot_id
    folder.mkdir(parents=True)
    key_path = paths.hmac_key
    if not key_path.exists():
        with key_path.open("xb") as stream:
            stream.write(secrets.token_bytes(32))
    report = {
        "snapshot_id": snapshot_id,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "rule_version": RULE_VERSION,
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
        statement = (
            files("shopvn.ingestion")
            .joinpath("sql/direct_revenue_baseline.sql")
            .read_text(encoding="utf-8")
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
    (paths.direct / "latest-snapshot.json").write_text(
        json.dumps({"snapshot_id": snapshot_id}, indent=2), encoding="utf-8"
    )
    print(
        f"Read-only snapshot {snapshot_id}: "
        + ", ".join(f"{k}={v['rows']}" for k, v in report["tables"].items()),
        flush=True,
    )
    return manifest


def main():
    extract()
