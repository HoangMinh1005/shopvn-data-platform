"""Export aggregate S2 proof. Never export credentials, raw lines or claim full freshness."""

import base64
import hashlib
import json
import subprocess
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

from shopvn.clients.trino import trino_query

ROOT = Path(__file__).resolve().parents[1]


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def get(url, headers=None):
    with urllib.request.urlopen(
        urllib.request.Request(url, headers=headers or {}), timeout=20
    ) as response:
        return json.load(response)


def main():
    runtime = ROOT / "runtime/stage2"
    reports = {
        name: read_json(path)
        for name, path in {
            "marketplace_spark": runtime / "spark-tests.json",
            "base_spark_regression": ROOT / "runtime/refactor/spark-layer-tests.json",
            "temporal_spark_regression": ROOT / "runtime/stage1/temporal-tests.json",
            "airflow_marketplace": runtime / "airflow.json",
            "direct_after_marketplace": runtime / "direct-regression.json",
            "three_fresh_processes": runtime / "replays.json",
            "final_actual_smoke": runtime / "final-smoke.json",
        }.items()
    }
    if any(r["status"] != "passed" for r in reports.values()):
        raise RuntimeError("Do not export incomplete/failed tests as PASS")
    suite = ET.parse(runtime / "unit-tests.xml").getroot().find("testsuite")
    unit = {k: int(suite.attrib[k]) for k in ("tests", "failures", "errors", "skipped")}
    if unit["failures"] or unit["errors"] or unit["skipped"]:
        raise RuntimeError("Unit verification incomplete")
    assert len(reports["three_fresh_processes"]["runs"]) == 3
    assert (
        reports["three_fresh_processes"]["before_snapshot"]
        == reports["three_fresh_processes"]["after_snapshot"]
    )
    assert (
        reports["airflow_marketplace"]["before_snapshot"]
        == reports["airflow_marketplace"]["after_blocked_snapshot"]
    )
    publication = trino_query(
        "SELECT DISTINCT publication_id FROM lakehouse.analytics.mart_commerce_finance_daily"
    )[0][0]
    assert publication == reports["final_actual_smoke"]["run_key"]
    candidate = read_json(ROOT / "runtime/marketplace/runs" / publication / "candidate.json")
    manifest = read_json(
        ROOT / "runtime/marketplace/landing" / candidate["capture_id"] / "manifest.json"
    )
    approval = read_json(ROOT / "runtime/marketplace/runs" / publication / "validation.json")
    assert approval["passed"] and approval["reconciliation_delta"] == "0.00"
    assert manifest["states"] == {"verified": 85, "missing": 3, "checksum_mismatch": 2}
    assert candidate["accepted_rows"] == 63103 and candidate["formula_warnings"] == 14
    assert candidate["excluded_status_rows"] == 7622
    assert trino_query(
        "SELECT bool_and(published_at>=prepared_at) FROM lakehouse.analytics.mart_commerce_finance_daily"
    ) == [[True]]
    assert trino_query(
        "SELECT DISTINCT source_candidate_snapshot_id FROM lakehouse.analytics.mart_commerce_finance_daily"
    ) == [[candidate["tables"]["mart"]["snapshot_id"]]]
    finance = trino_query(
        "SELECT channel,CAST(sum(comparable_merchandise) AS VARCHAR),CAST(sum(partner_reported_net) AS VARCHAR),sum(eligible_orders),sum(units_sold) FROM lakehouse.analytics.mart_commerce_finance_daily WHERE report_type='sales' GROUP BY 1 ORDER BY 1"
    )
    counts = trino_query(
        "SELECT source_system,count(*),count(DISTINCT sales_line_key) FROM lakehouse.gold.fact_sales_line GROUP BY 1 ORDER BY 1"
    )
    assert sum(r[1] for r in counts if r[0].startswith("marketplace/")) == 63103
    assert all(r[1] == r[2] for r in counts)
    assert trino_query(
        "SELECT CAST(sum(net_customer_amount) AS VARCHAR) FROM lakehouse.analytics.mart_fin02_daily"
    ) == [["1648602104364.00"]]
    missing = trino_query(
        "SELECT CAST(business_date AS VARCHAR),channel,coverage_status,partner_reported_net FROM lakehouse.analytics.marketplace_finance_daily WHERE coverage_status<>'verified' ORDER BY 1,2"
    )
    assert len(missing) == 5 and all(r[3] is None for r in missing)
    status = get("http://localhost:19108/api/status")
    assert all(status["services"].values()) and len(status["services"]) == 9
    access = read_json(ROOT / "runtime/airflow/local-access.json")
    auth = "Basic " + base64.b64encode(("admin:" + access["grafana_password"]).encode()).decode()
    dashboard = get(
        access["grafana_url"] + "/api/dashboards/uid/shopvn-operations", {"Authorization": auth}
    )
    assert len(dashboard["dashboard"]["panels"]) == 31 and dashboard["meta"]["provisioned"]
    rules = get("http://localhost:19090/api/v1/rules")
    rule_names = [r["name"] for g in rules["data"]["groups"] for r in g["rules"]]
    assert len(rule_names) == 13
    wheel = runtime / "package/shopvn_data_platform-0.1.0-py3-none-any.whl"
    with ZipFile(wheel) as archive:
        for path in (ROOT / "src/shopvn").rglob("*.py"):
            assert archive.read(path.relative_to(ROOT / "src").as_posix()) == path.read_bytes(), (
                "Wheel/source mismatch: " + str(path)
            )
    subprocess.run(
        [
            str(runtime / "wheel-venv/Scripts/python.exe"),
            "-I",
            "-c",
            "import shopvn.jobs.marketplace,shopvn.jobs.direct_finance;from importlib.resources import files;assert files('shopvn.ingestion').joinpath('sql/direct_revenue_baseline.sql').is_file()",
        ],
        check=True,
    )
    report = {
        "status": "passed",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "S2 actual SFTP + marketplace/cross-channel Finance; local single node; no API/Kafka/Superset",
        "unit_tests": unit,
        **reports,
        "source": {
            "capture_id": candidate["capture_id"],
            "observed_at": manifest["observed_at"],
            "completed_at": manifest["completed_at"],
            "states": manifest["states"],
            "accepted_rows": candidate["accepted_rows"],
            "excluded_status_rows": candidate["excluded_status_rows"],
            "formula_warnings": candidate["formula_warnings"],
        },
        "publication": {
            "publication_id": publication,
            "approval": approval,
            "sales_line_source_counts": counts,
            "finance_columns": [
                "channel",
                "merchandise",
                "reported_net",
                "eligible_orders",
                "units",
            ],
            "finance": finance,
            "unavailable_scope_columns": ["business_date", "channel", "state", "reported_net"],
            "unavailable_scopes": missing,
            "registry_snapshot_id": candidate["tables"]["registry"]["snapshot_id"],
        },
        "monitoring": {
            "services": status["services"],
            "latest_marketplace": status["marketplace"],
            "grafana_panels": 31,
            "prometheus_rules": rule_names,
            "open_alerts": status["open_alerts"],
        },
        "packaging": {
            "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "source_matches_wheel": True,
            "isolated_job_imports_and_sql": "passed",
        },
        "review": "Implementation/self-verification complete; independent team review and stage-close approval remain with user",
        "limits": [
            "June marketplace is partial (3 missing, 2 checksum mismatch); never substitute zero",
            "Marketplace reported net is not bank cash; settlement T+15 is only scheduled",
            "Late arrival is fixture/mock-clock verification, not actual delayed arrival from baked source",
            "Certified Finance table commits atomically; all Gold tables are not one transaction",
            "Frozen June inputs do not prove current November D-1 freshness/SLA",
            "Observed SCD/actual inventory rules and no-June-stock-backfill remain unchanged",
            "No outbound notifications, API ingestion, Kafka, Superset or production HA certification",
        ],
    }
    (ROOT / "docs/verification/stage2.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    print(
        "Exported S2 PASS with actual source/DAG/Trino, replay/regression, packaging and scope limits"
    )


if __name__ == "__main__":
    main()
