"""Three fresh Spark candidates on one immutable actual SFTP capture, never publishing."""

import json
import subprocess
import uuid
from pathlib import Path

from shopvn.clients.trino import trino_query
from shopvn.quality.marketplace import validate

ROOT = Path(__file__).resolve().parents[2]


def certified():
    return trino_query(
        'SELECT snapshot_id FROM lakehouse.analytics."mart_commerce_finance_daily$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]


def main():
    publication = trino_query(
        "SELECT DISTINCT publication_id FROM lakehouse.analytics.mart_commerce_finance_daily"
    )[0][0]
    saved = json.loads(
        (ROOT / "runtime/marketplace/runs" / publication / "candidate.json").read_text()
    )
    capture_id = saved["capture_id"]
    report = {
        "status": "running",
        "capture_id": capture_id,
        "before_snapshot": certified(),
        "runs": [],
    }
    try:
        for _ in range(3):
            key = uuid.uuid4().hex[:24]
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
                    "/workspace/scripts/marketplace_spark.py",
                    "prepare",
                    "--capture-id",
                    capture_id,
                    "--run-key",
                    key,
                ],
                check=True,
                timeout=600,
            )
            candidate = json.loads(
                (ROOT / "runtime/marketplace/runs" / key / "candidate.json").read_text()
            )
            approval = validate(key)
            assert candidate["accepted_rows"] == 63103 and candidate["marketplace_rows"] == 63103
            assert approval["reported_net_total"] == "606161750704.00"
            assert certified() == report["before_snapshot"]
            report["runs"].append(
                {
                    "run_key": key,
                    "accepted_rows": candidate["accepted_rows"],
                    "marketplace_rows": candidate["marketplace_rows"],
                    "approval": approval,
                }
            )
        report.update(status="passed", after_snapshot=certified())
    finally:
        (ROOT / "runtime/stage2/replays.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
