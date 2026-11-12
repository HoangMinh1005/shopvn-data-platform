"""Export stage-1 aggregate evidence, including actual (not simulated) EOD capture."""

import hashlib
import json
import subprocess
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from verify_airflow import Airflow

from shopvn.clients.trino import trino_query
from shopvn.contracts.direct import SCHEMAS

ROOT = Path(__file__).resolve().parents[1]


def main():
    runtime = ROOT / "runtime/stage1"
    results = {
        name: json.loads(path.read_text())
        for name, path in {
            "temporal_tests": runtime / "temporal-tests.json",
            "base_layer_tests": ROOT / "runtime/refactor/spark-layer-tests.json",
            "replays": runtime / "replays.json",
            "finance_airflow": ROOT / "runtime/airflow/verification.json",
            "actual_eod_replay": runtime / "actual-eod-replay.json",
            "airflow_clock_tests": runtime / "airflow-clock-tests.json",
        }.items()
    }
    if any(report["status"] != "passed" for report in results.values()):
        raise RuntimeError("Do not export incomplete/failed Stage 1 evidence")
    suite = ET.parse(runtime / "unit-tests.xml").getroot().find("testsuite")
    unit = {name: int(suite.attrib[name]) for name in ("tests", "failures", "errors", "skipped")}
    if unit["failures"] or unit["errors"] or unit["skipped"]:
        raise RuntimeError("Unit suite is incomplete")
    if len(results["replays"]["runs"]) != 3:
        raise RuntimeError("Three fresh process replays are required")
    snapshot = results["replays"]["snapshot_id"]
    manifest = json.loads(
        (ROOT / "runtime/direct/landing" / snapshot / "manifest.json").read_text()
    )
    if (
        set(manifest["tables"]) != set(SCHEMAS)
        or len(SCHEMAS) != 9
        or manifest["source_transaction"]["read_only"] != "on"
    ):
        raise RuntimeError("Nine-table read-only source proof missing")
    with urllib.request.urlopen("http://localhost:19108/api/status", timeout=10) as response:
        status = json.load(response)
    inventory = status["inventory"]
    if (
        inventory["status"] != "success"
        or not inventory["eod_published"]
        or not 0 <= inventory["capture_lag_seconds"] < 60
    ):
        raise RuntimeError("Actual EOD source observation was not verified within approved window")
    api = Airflow(json.loads((ROOT / "runtime/airflow/local-access.json").read_text()))
    dag_run = api.call("dags/shopvn_inventory_observation/dagRuns/" + inventory["dag_run_id"])
    if dag_run["state"] != "success" or dag_run["run_type"] != "scheduled":
        raise RuntimeError("Require actual scheduled midnight DAG success, not a synthetic clock")
    policy = trino_query(
        "SELECT CAST(go_live_date AS VARCHAR),window_seconds,policy_version,CAST(first_observed_at AS VARCHAR) FROM lakehouse.ops.inventory_policy"
    )
    daily = trino_query(
        "SELECT CAST(snapshot_date AS VARCHAR),count(*),sum(stock_qty),min(capture_lag_seconds),max(capture_lag_seconds),CAST(min(observed_at) AS VARCHAR),CAST(min(snapshot_cutoff_at) AS VARCHAR) FROM lakehouse.gold.fact_inventory_daily_snapshot GROUP BY 1 ORDER BY 1"
    )
    if any(row[0] < policy[0][0] for row in daily):
        raise RuntimeError("Inventory history was backfilled before actual go-live")
    stock = trino_query(
        "SELECT count(*),sum(stock_qty),CAST(max(observed_at) AS VARCHAR) FROM lakehouse.analytics.current_stock"
    )[0]
    if stock[:2] != [inventory["rows"], inventory["stock_qty"]]:
        raise RuntimeError("Inventory published/source totals differ")
    wheel = runtime / "package/shopvn_data_platform-0.1.0-py3-none-any.whl"
    python = runtime / "wheel-venv/Scripts/python.exe"
    subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import shopvn.jobs.inventory,shopvn.jobs.direct_finance;"
            "from importlib.resources import files;"
            "assert files('shopvn.ingestion').joinpath('sql/direct_revenue_baseline.sql').is_file()",
        ],
        check=True,
    )
    report = {
        "status": "passed",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Stage 1: 9 PostgreSQL tables, observed-time SCD2/day-asof, actual current/EOD stock; no historic stock reconstruction",
        "unit_tests": unit,
        "temporal_spark_tests": results["temporal_tests"],
        "base_spark_tests": results["base_layer_tests"],
        "airflow_clock_tests": results["airflow_clock_tests"],
        "packaging": {
            "isolated_wheel_job_imports_and_sql_resource": "passed",
            "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        },
        "source": {
            "snapshot_id": snapshot,
            "contract_version": manifest["contract_version"],
            "observed_at_utc": manifest["observed_at_utc"],
            "transaction": manifest["source_transaction"],
            "counts": {name: value["rows"] for name, value in manifest["tables"].items()},
            "review_comment_exported": False,
        },
        "finance": {
            "fin02_total_vnd": results["finance_airflow"]["fin02_total_vnd"],
            "replays": 3,
            "replay_fin02_totals": [r["fin02_total_vnd"] for r in results["replays"]["runs"]],
            "dq_block_kept_certified_snapshot": results["finance_airflow"][
                "publication_unchanged_on_failure"
            ],
            "recovery": results["finance_airflow"]["recovery"],
        },
        "inventory": {
            "policy_columns": [
                "go_live_date",
                "window_seconds",
                "policy_version",
                "first_observed_at",
            ],
            "policy": policy,
            "scheduled_dag_run": inventory,
            "current_stock": stock,
            "daily_columns": [
                "business_date",
                "rows",
                "stock_qty",
                "min_capture_lag_seconds",
                "max_capture_lag_seconds",
                "observed_at_utc",
                "cutoff_at_utc",
            ],
            "daily": daily,
            "actual_replay": results["actual_eod_replay"],
        },
        "services": status["services"],
        "runtime_health_at_export": {
            "all_observed_components_healthy": all(status["services"].values()),
            "latest_finance_run": status["latest_run"],
            "note": "Acceptance PASS records completed checks; it does not declare current service health or current-month Finance freshness green",
        },
        "open_alerts": status["open_alerts"],
        "limits": [
            "SCD2 is observed-time; date-only order lookup is not exact purchase-time tier/cost",
            "EOD is an actual observation within an approved 60s window, not exact stock at 00:00",
            "June stock backfill remains blocked",
            "Historical apply of stale Finance snapshots is blocked; use fresh source snapshots for restatement",
            "API/SFTP/Kafka/Superset remain outside Stage 1",
            "Scheduled Finance requires current source coverage; the supplied orders only cover June 2026",
        ],
    }
    (ROOT / "docs/verification/stage1.json").write_text(
        json.dumps(report, default=str, indent=2), encoding="utf-8"
    )
    print(
        "Exported Stage 1 PASS: nine-source counts, SCD/day-asof, real scheduled EOD and actual-capture replay"
    )


if __name__ == "__main__":
    main()
