"""Export aggregate runtime proof and exact tested image identities, never credentials."""

import base64
import json
import subprocess
from pathlib import Path

from verify_airflow import request

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "--project-name",
    "shopvn-ops",
    "--env-file",
    str(ROOT / ".env"),
    "-f",
    str(ROOT / "infra/platform/airflow/compose.yml"),
]


def main():
    evidence = json.loads((ROOT / "runtime/airflow/verification.json").read_text())
    if evidence["status"] != "passed":
        raise RuntimeError("Do not export failed or incomplete evidence as successful")
    run_log = (
        ROOT
        / "runtime/airflow/logs/dag_id=shopvn_direct_finance"
        / ("run_id=" + evidence["recovery"]["dag_run_id"])
        / "task_id=transform_candidate/attempt=1.log"
    )
    if "Running Spark version 3.5.9" not in run_log.read_text(encoding="utf-8"):
        raise RuntimeError("Actual scheduled task did not confirm the expected Spark engine")
    evidence["spark_engine_observed_in_task_log"] = "3.5.9"
    access = json.loads((ROOT / "runtime/airflow/local-access.json").read_text())
    basic = base64.b64encode(("admin:" + access["grafana_password"]).encode()).decode()
    result = request(
        access["grafana_url"]
        + "/api/datasources/proxy/uid/shopvn-prometheus/api/v1/query?query=shopvn_service_up",
        headers={"Authorization": "Basic " + basic},
    )
    assert result["status"] == "success" and len(result["data"]["result"]) == 8
    assert all(metric["value"][1] == "1" for metric in result["data"]["result"])
    evidence["grafana"]["prometheus_proxy_query"] = (
        "passed; 8 observed service/component metrics healthy"
    )
    code = """import hashlib,json,importlib.metadata as m
from pathlib import Path
print(json.dumps({'packages':{n:m.version(n) for n in ['apache-airflow','apache-airflow-providers-apache-spark','apache-airflow-providers-postgres','apache-airflow-providers-standard','prometheus-client','psycopg','pyspark-client']},'constraints_sha256':hashlib.sha256(Path('/opt/airflow-constraints.txt').read_bytes()).hexdigest()}))"""
    packages = json.loads(
        subprocess.check_output(
            COMPOSE + ["exec", "-T", "airflow-scheduler", "python", "-c", code], text=True
        )
    )
    images = {}
    for service in (
        "airflow-api",
        "airflow-scheduler",
        "airflow-dag-processor",
        "pipeline-monitor",
        "prometheus",
        "grafana",
    ):
        container = subprocess.check_output(COMPOSE + ["ps", "-q", service], text=True).strip()
        details = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[
            0
        ]
        images[service] = {
            "reference": details["Config"]["Image"],
            "image_id": details["Image"],
            "oom_killed": details["State"]["OOMKilled"],
        }
        assert not images[service]["oom_killed"]
    lock = {
        "scope": "Local direct Finance orchestration and service/DQ monitoring; not production, Kafka or Superset",
        **packages,
        "images": images,
        "spark_engine": "3.5.9; base Python 3.11.15; not pyspark-client 4.3.0",
    }
    (ROOT / "infra/platform/airflow/verified-lock.json").write_text(
        json.dumps(lock, indent=2), encoding="utf-8"
    )
    (ROOT / "docs/verification/airflow-observability.json").write_text(
        json.dumps(evidence, indent=2), encoding="utf-8"
    )
    print("Exported sanitized Airflow/observability evidence and tested image/package lock")


if __name__ == "__main__":
    main()
