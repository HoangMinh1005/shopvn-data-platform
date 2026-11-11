"""Verify refactored layers against a saved source snapshot without publishing a mart."""

import argparse
import hashlib
import json
import subprocess
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from shopvn.clients.trino import trino_query
from shopvn.quality.finance import assert_fin02_matches, expected_fin02, queried_fin02

ROOT = Path(__file__).resolve().parents[2]


def certified_snapshot():
    return trino_query(
        'SELECT snapshot_id FROM lakehouse.analytics."mart_fin02_daily$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-id")
    args = parser.parse_args()
    snapshot_id = (
        args.snapshot_id
        or json.loads((ROOT / "runtime/direct/latest-snapshot.json").read_text())["snapshot_id"]
    )
    manifest = json.loads(
        (ROOT / "runtime/direct/landing" / snapshot_id / "manifest.json").read_text()
    )
    key = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:24]
    output = ROOT / "runtime/refactor"
    output.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot_id,
        "run_key": key,
        "source": "saved immutable read-only source snapshot; no new live extraction",
    }
    try:
        before = certified_snapshot()
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
                "/workspace/scripts/direct_sales_spark.py",
                "--snapshot-id",
                snapshot_id,
                "--candidate-name",
                "mart_fin02_" + key,
                "--publication-id",
                key,
                "--result-path",
                "/workspace/runtime/refactor/transform.json",
            ],
            check=True,
            timeout=900,
        )
        transformed = json.loads((output / "transform.json").read_text())
        if transformed["status"] != "passed":
            raise RuntimeError("Refactored Spark pipeline failed")
        candidate = int(transformed["candidate_snapshot_id"])
        rows = trino_query(
            f"SELECT CAST(order_date AS VARCHAR),channel,eligible_order_count,CAST(net_customer_amount AS VARCHAR) FROM lakehouse.staging.mart_fin02_{key} FOR VERSION AS OF {candidate} ORDER BY 1,2"
        )
        actual = queried_fin02(rows)
        assert_fin02_matches(actual, expected_fin02(manifest))
        after = certified_snapshot()
        if before != after:
            raise RuntimeError("Candidate-only replay unexpectedly changed the certified mart")
        report.update(
            status="passed",
            spark=transformed,
            trino_rows=len(rows),
            fin02_total_vnd=str(sum((value[1] for value in actual.values()), Decimal("0"))),
            certified_before=before,
            certified_after=after,
            finished_at_utc=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        report.update(status="failed", error_class=type(exc).__name__)
        raise
    finally:
        (output / "direct-replay.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("Refactor replay report:", report["status"], flush=True)


if __name__ == "__main__":
    main()
