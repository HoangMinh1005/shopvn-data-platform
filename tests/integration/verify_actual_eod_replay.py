"""Replay an actual accepted midnight capture; never edits its observation clock."""

import json
import subprocess
import urllib.request
from pathlib import Path

from shopvn.clients.trino import trino_query

ROOT = Path(__file__).resolve().parents[2]


def main():
    with urllib.request.urlopen("http://localhost:19108/api/status", timeout=10) as response:
        latest = json.load(response)["inventory"]
    if latest["status"] != "success" or not latest["eod_published"]:
        raise RuntimeError("An actual successful EOD capture is required; do not fabricate one")
    day = latest["business_date"]
    before = trino_query(
        'SELECT snapshot_id FROM lakehouse.gold."fact_inventory_daily_snapshot$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]
    subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            "shopvn-readiness",
            "--env-file",
            str(ROOT / ".env"),
            "-f",
            str(ROOT / "infra/platform/spike/compose.yml"),
            "run",
            "--rm",
            "--no-deps",
            "spark",
            "--master",
            "local[2]",
            "--driver-memory",
            "1g",
            "/workspace/scripts/inventory_spark.py",
            "--snapshot-id",
            latest["snapshot_id"],
            "--mode",
            "eod",
            "--business-date",
            day,
            "--result-path",
            "/workspace/runtime/stage1/eod-replay-result.json",
        ],
        check=True,
        timeout=600,
    )
    result = json.loads((ROOT / "runtime/stage1/eod-replay-result.json").read_text())
    after = trino_query(
        'SELECT snapshot_id FROM lakehouse.gold."fact_inventory_daily_snapshot$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]
    rows = trino_query(
        f"SELECT count(*),sum(stock_qty) FROM lakehouse.gold.fact_inventory_daily_snapshot WHERE snapshot_date=DATE '{day}'"
    )[0]
    if (
        result["status"] != "passed"
        or not result["eod_published"]
        or before != after
        or rows != [latest["rows"], latest["stock_qty"]]
    ):
        raise RuntimeError("Actual EOD replay changed the accepted capture")
    (ROOT / "runtime/stage1/actual-eod-replay.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "snapshot_id": latest["snapshot_id"],
                "business_date": day,
                "before_iceberg_snapshot": before,
                "after_iceberg_snapshot": after,
                "rows": rows[0],
                "stock_qty": rows[1],
                "capture_lag_seconds": result["capture"]["capture_lag_seconds"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("Actual EOD replay passed; accepted daily Iceberg snapshot unchanged")


if __name__ == "__main__":
    main()
