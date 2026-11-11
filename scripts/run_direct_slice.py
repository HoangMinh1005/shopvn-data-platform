"""Validate the direct Finance slice in three fresh Spark jobs and query its mart with Trino."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from extract_direct_snapshot import extract
from run_lakehouse_spike import compose, trino_query, wait_http

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-id", help="Replay an existing complete landing snapshot")
    parser.add_argument("--keep-running", action="store_true")
    args = parser.parse_args()
    spike = json.loads((ROOT / "runtime/spike/spike-report.json").read_text())
    if spike["status"] != "passed":
        raise RuntimeError("Complete the storage spike before running the direct slice")
    manifest_path = (ROOT / "runtime/direct/landing" / args.snapshot_id / "manifest.json") if args.snapshot_id else extract()
    manifest = json.loads(manifest_path.read_text())
    snapshot_id = manifest["snapshot_id"]
    report = {"started_at_utc": datetime.now(timezone.utc).isoformat(), "snapshot_id": snapshot_id,
              "rule_version": "1.1", "runs": [], "status": "running"}
    output = ROOT / "runtime/direct"
    try:
        compose("up", "-d", "catalog", "minio", "trino")
        wait_http("http://127.0.0.1:19000/minio/health/live")
        wait_http("http://127.0.0.1:18080/v1/info")
        for index in range(1, 4):
            print(f"Direct Finance fresh Spark process {index}/3", flush=True)
            compose("run", "--rm", "--no-deps", "spark", "--master", "local[2]", "--driver-memory", "1g",
                    "/workspace/scripts/direct_sales_spark.py", "--snapshot-id", snapshot_id, timeout=900)
            metrics = json.loads((output / "direct-result.json").read_text())
            if metrics["status"] != "passed":
                raise RuntimeError("Direct job did not pass")
            report["runs"].append(metrics)
        compared = [tuple(row[key] if key != "fact_counts" else sorted(row[key].items())
                          for key in ("fin02_total", "eligible_orders", "mart_rows", "fact_counts"))
                    for row in report["runs"]]
        if not compared[0] == compared[1] == compared[2]:
            raise ValueError("Direct replay count/amount invariants differ")
        rows = trino_query("""SELECT CAST(order_date AS VARCHAR),channel,eligible_order_count,
                           CAST(net_customer_amount AS VARCHAR),CAST(refunded_order_amount AS VARCHAR)
                           FROM lakehouse.analytics.mart_fin02_daily ORDER BY 1,2""")
        expected = {(row["order_date"], row["channel"]): (row["eligible_order_count"], Decimal(row["net_customer_amount"]))
                    for row in manifest["fin02_expected_by_day_channel"]}
        actual = {(row[0], row[1]): (row[2], Decimal(row[3])) for row in rows}
        if actual != expected:
            raise ValueError("Trino-to-source FIN02 reconciliation failed")
        (output / "fin02-trino.json").write_text(json.dumps({"columns": ["order_date", "channel", "eligible_orders", "net_customer_amount", "refunded_order_amount"], "rows": rows}, indent=2), encoding="utf-8")
        report.update(status="passed", trino_rows=len(rows), trino_fin02_total=str(sum(Decimal(row[3]) for row in rows)),
                      source_transaction=manifest["source_transaction"], finished_at_utc=datetime.now(timezone.utc).isoformat())
        print("Direct Finance slice passed: source / Spark / Trino and 3 fresh-process replays", flush=True)
    except Exception as exc:
        report.update(status="failed", failure=str(exc))
        raise
    finally:
        (output / "direct-slice-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        if not args.keep_running:
            compose("stop", "catalog", "minio", "trino", timeout=120)


if __name__ == "__main__":
    main()
