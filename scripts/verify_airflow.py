"""Exercise the actual scheduler, DQ gate, retries and local monitoring (writes test DAG runs)."""

from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from shopvn.clients.trino import trino_query

ROOT = Path(__file__).resolve().parents[1]
DAG = "shopvn_direct_finance"


def request(url, body=None, headers=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


class Airflow:
    def __init__(self, access):
        self.url = access["airflow_url"]
        token = request(
            self.url + "/auth/token",
            {"username": access["username"], "password": access["airflow_password"]},
        )["access_token"]
        self.headers = {"Authorization": "Bearer " + token}

    def call(self, path, body=None):
        return request(self.url + "/api/v2/" + path, body, self.headers)

    def trigger(self, name, day, **flags):
        run_id = "manual__" + name + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        self.call(
            f"dags/{DAG}/dagRuns",
            {"dag_run_id": run_id, "logical_date": None, "conf": {"business_date": day, **flags}},
        )
        return run_id

    def wait(self, run_id, timeout=900):
        limit = time.monotonic() + timeout
        previous = None
        while time.monotonic() < limit:
            run = self.call(f"dags/{DAG}/dagRuns/{run_id}")
            tasks = self.call(f"dags/{DAG}/dagRuns/{run_id}/taskInstances")["task_instances"]
            brief = [(t["task_id"], t["state"], t["try_number"]) for t in tasks]
            if brief != previous:
                print(run_id, run["state"], brief, flush=True)
                previous = brief
            if run["state"] in {"success", "failed"}:
                return {"dag_run_id": run_id, "state": run["state"], "tasks": brief}
            time.sleep(5)
        raise TimeoutError("Actual Airflow run did not finish within smoke-test timeout")


def certified_snapshot():
    return trino_query(
        'SELECT snapshot_id FROM lakehouse.analytics."mart_fin02_daily$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0][0]


def prom(query):
    return request(
        "http://localhost:19090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    )["data"]["result"]


def wait_for(predicate, timeout=90):
    limit = time.monotonic() + timeout
    while time.monotonic() < limit:
        result = predicate()
        if result:
            return result
        time.sleep(5)
    raise TimeoutError("Monitoring assertion did not become true")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--business-date", default="2026-06-30")
    parser.add_argument(
        "--resume-blocked-run", help="Resume observing an already triggered intentional failure"
    )
    args = parser.parse_args()
    access = json.loads((ROOT / "runtime/airflow/local-access.json").read_text())
    api = Airflow(access)
    output = ROOT / "runtime/airflow/verification.json"
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "business_date": args.business_date,
        "scope": "actual LocalExecutor/Spark/Trino; local Prometheus/Grafana; no Kafka, Superset or outbound notifications",
    }
    try:
        before = certified_snapshot()
        blocked_id = args.resume_blocked_run or api.trigger(
            "verify_blocked", args.business_date, force_validation_failure=True
        )
        report["blocked"] = api.wait(blocked_id)
        assert report["blocked"]["state"] == "failed"
        blocked_tasks = {task: (state, tries) for task, state, tries in report["blocked"]["tasks"]}
        assert blocked_tasks["extract_snapshot"][0] == "success"
        assert blocked_tasks["transform_candidate"][0] == "success"
        assert blocked_tasks["validate_candidate"] == ("failed", 3)
        assert blocked_tasks["publish_finance"][0] == "upstream_failed"
        assert blocked_tasks["finalize"][0] == "failed"
        after = certified_snapshot()
        assert before == after, "Rejected candidate changed the certified mart"
        report["publication_unchanged_on_failure"] = {
            "before_snapshot_id": before,
            "after_snapshot_id": after,
        }
        report["firing_failure_alerts"] = wait_for(
            lambda: prom('ALERTS{alertname="ShopVNPipelineFailed",alertstate="firing"}')
        )
        recovery = api.trigger("verify_recovery", args.business_date, validation_fail_once=True)
        report["recovery"] = api.wait(recovery)
        assert report["recovery"]["state"] == "success"
        assert (
            dict((task, tries) for task, _, tries in report["recovery"]["tasks"])[
                "validate_candidate"
            ]
            == 2
        )
        wait_for(lambda: prom('shopvn_pipeline_state{state="success"}')[0]["value"][1] == "1")
        status = request("http://localhost:19108/api/status")
        assert all(status["services"].values()), "One of the observed services is unhealthy"
        assert status["latest_run"]["published"] and status["latest_run"]["dq_passed"]
        assert status["latest_run"]["amount_delta"] == "0.00"
        from shopvn.orchestration.ops import run_key

        tested_keys = {run_key(blocked_id), run_key(recovery)}
        assert not any(a["run_key"] in tested_keys for a in status["open_alerts_by_run"]), (
            "Failure alerts for the tested direct runs were not resolved"
        )
        report["monitor_status"] = status
        report["certified_snapshot_id"] = certified_snapshot()
        report["fin02_total_vnd"] = trino_query(
            "SELECT CAST(sum(net_customer_amount) AS VARCHAR) FROM lakehouse.analytics.mart_fin02_daily"
        )[0][0]
        credentials = base64.b64encode(("admin:" + access["grafana_password"]).encode()).decode()
        dashboard = request(
            access["grafana_url"] + "/api/dashboards/uid/shopvn-operations",
            headers={"Authorization": "Basic " + credentials},
        )
        report["grafana"] = {
            "title": dashboard["dashboard"]["title"],
            "panels": len(dashboard["dashboard"]["panels"]),
            "provisioned": dashboard["meta"]["provisioned"],
        }
        assert report["grafana"]["provisioned"]
        report["deadline_verification"] = (
            "08:00 policy covered by fixed-clock unit tests; native 30-minute expiry not awaited in this smoke test"
        )
        report.update(status="passed", finished_at_utc=datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        report.update(status="failed", error_class=type(exc).__name__)
        raise
    finally:
        output.write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
        print("Sanitized verification report:", output, flush=True)


if __name__ == "__main__":
    main()
