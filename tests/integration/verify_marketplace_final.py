"""Final actual DAG smoke after publication-lineage hardening; no failure bypass."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_airflow import Airflow  # noqa: E402
from verify_marketplace_airflow import DAG, wait  # noqa: E402

from shopvn.orchestration.ops import run_key  # noqa: E402


def main():
    api = Airflow(json.loads((ROOT / "runtime/airflow/local-access.json").read_text()))
    run = "manual__s2_final_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    api.call(
        f"dags/{DAG}/dagRuns",
        {
            "dag_run_id": run,
            "logical_date": None,
            "conf": {"start_date": "2026-06-01", "end_date": "2026-06-30"},
        },
    )
    result = wait(api, run)
    assert result["state"] == "success"
    result.update(status="passed", run_key=run_key(run, DAG))
    (ROOT / "runtime/stage2/final-smoke.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
