"""Actual SFTP/Airflow/Spark/Trino failure → recovery; leaves source files unchanged."""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from shopvn.clients.trino import trino_query

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from verify_airflow import Airflow  # noqa: E402

DAG = "shopvn_marketplace_finance"


def certified():
    if not trino_query("SHOW TABLES FROM lakehouse.analytics LIKE 'mart_commerce_finance_daily'"):
        return None
    return trino_query(
        'SELECT snapshot_id FROM lakehouse.analytics."mart_commerce_finance_daily$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]


def wait(api, run, timeout=900):
    limit, previous = time.monotonic() + timeout, None
    while time.monotonic() < limit:
        state = api.call(f"dags/{DAG}/dagRuns/{run}")
        tasks = api.call(f"dags/{DAG}/dagRuns/{run}/taskInstances")["task_instances"]
        brief = [(t["task_id"], t["state"], t["try_number"]) for t in tasks]
        if brief != previous:
            print(run, state["state"], brief, flush=True)
            previous = brief
        if state["state"] in {"success", "failed"}:
            return {"run_id": run, "state": state["state"], "tasks": brief}
        time.sleep(5)
    raise TimeoutError("Marketplace DAG did not finish")


def main():
    root = ROOT / "runtime/stage2"
    root.mkdir(parents=True, exist_ok=True)
    api = Airflow(json.loads((ROOT / "runtime/airflow/local-access.json").read_text()))
    report = {
        "status": "running",
        "source": "Actual restored SFTP; no source mutations",
        "before_snapshot": certified(),
    }
    try:
        for name, flags in [
            ("blocked", {"force_validation_failure": True}),
            ("recovery", {"validation_fail_once": True}),
        ]:
            run = (
                "manual__s2_" + name + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            )
            api.call(
                f"dags/{DAG}/dagRuns",
                {
                    "dag_run_id": run,
                    "logical_date": None,
                    "conf": {"start_date": "2026-06-01", "end_date": "2026-06-30", **flags},
                },
            )
            report[name] = wait(api, run)
            if name == "blocked":
                assert report[name]["state"] == "failed"
                assert any(
                    t[0] == "prepare_candidate" and t[1] == "success" for t in report[name]["tasks"]
                )
                assert any(
                    t[0] == "validate_candidate" and t[1] == "failed" and t[2] == 3
                    for t in report[name]["tasks"]
                )
                report["after_blocked_snapshot"] = certified()
                assert report["after_blocked_snapshot"] == report["before_snapshot"]
            else:
                assert report[name]["state"] == "success"
                assert any(
                    t[0] == "validate_candidate" and t[1] == "success" and t[2] == 2
                    for t in report[name]["tasks"]
                )
        report["after_recovery_snapshot"] = certified()
        report["published_totals"] = trino_query(
            "SELECT channel,count(*),CAST(sum(comparable_merchandise) AS VARCHAR),CAST(sum(partner_reported_net) AS VARCHAR) FROM lakehouse.analytics.mart_commerce_finance_daily WHERE report_type='sales' GROUP BY 1 ORDER BY 1"
        )
        report["status"] = "passed"
    finally:
        (root / "airflow.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
