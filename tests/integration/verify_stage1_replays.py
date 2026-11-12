"""Three independent source-snapshot replays; candidate-only, never replaces certified mart."""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    snapshot = json.loads((ROOT / "runtime/direct/latest-snapshot.json").read_text())["snapshot_id"]
    output = ROOT / "runtime/stage1"
    output.mkdir(parents=True, exist_ok=True)
    reports = []
    for index in range(3):
        print(f"Stage 1 fresh-process replay {index + 1}/3", flush=True)
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "tests/integration/verify_direct_replay.py"),
                "--snapshot-id",
                snapshot,
            ],
            check=True,
            cwd=ROOT,
        )
        report = json.loads((ROOT / "runtime/refactor/direct-replay.json").read_text())
        if report["status"] != "passed":
            raise RuntimeError("Stage 1 replay failed")
        reports.append(report)
    invariant = [
        (row["fin02_total_vnd"], row["spark"]["eligible_orders"], row["spark"]["fact_counts"])
        for row in reports
    ]
    if not invariant[0] == invariant[1] == invariant[2]:
        raise RuntimeError("Replay business invariants changed")
    (output / "replays.json").write_text(
        json.dumps({"status": "passed", "snapshot_id": snapshot, "runs": reports}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
