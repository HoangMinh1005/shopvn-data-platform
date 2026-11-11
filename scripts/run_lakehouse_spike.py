"""Run three fresh Spark jobs, compare Trino and verify persistence after restart."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".env"),
           "-f", str(ROOT / "infra/platform/spike/compose.yml")]


def compose(*arguments: str, timeout: int = 600):
    return subprocess.run(COMPOSE + list(arguments), cwd=ROOT, check=True, timeout=timeout)


def wait_http(url: str, timeout: int = 180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    if url.endswith("/v1/info") and json.load(response).get("starting", True):
                        time.sleep(2)
                        continue
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Service not ready: {url}")


def trino_query(statement: str) -> list:
    request = urllib.request.Request("http://127.0.0.1:18080/v1/statement",
        data=statement.encode(), headers={"X-Trino-User": "readiness", "Content-Type": "text/plain"})
    result = []
    deadline = time.monotonic() + 180
    while True:
        if time.monotonic() > deadline:
            raise TimeoutError("Trino query timeout")
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
        if "error" in payload:
            raise RuntimeError(payload["error"]["message"])
        result.extend(payload.get("data", []))
        if "nextUri" not in payload:
            return result
        request = urllib.request.Request(payload["nextUri"], headers={"X-Trino-User": "readiness"})
        time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-running", action="store_true", help="Leave isolated spike services up")
    args = parser.parse_args()
    output = ROOT / "runtime/spike"
    output.mkdir(parents=True, exist_ok=True)
    report = {"observed_at_utc": datetime.now(timezone.utc).isoformat(), "runs": [], "status": "running"}
    try:
        compose("up", "-d", "catalog", "minio", "trino")
        wait_http("http://127.0.0.1:19000/minio/health/live")
        wait_http("http://127.0.0.1:18080/v1/info")
        for run in range(1, 4):
            print(f"Fresh Spark job {run}/3", flush=True)
            compose("run", "--rm", "--no-deps", "spark")
            metrics = json.loads((output / "spark-result.json").read_text())
            metrics["run"] = run
            report["runs"].append(metrics)
        before = trino_query("SELECT count(*), CAST(sum(amount) AS VARCHAR), count(distinct line_id) FROM lakehouse.readiness.sales_probe")
        assert before == [[2, "325.00", 2]], before
        report["trino_before_restart"] = before
        compose("restart", "catalog", "minio", "trino")
        wait_http("http://127.0.0.1:19000/minio/health/live")
        wait_http("http://127.0.0.1:18080/v1/info")
        after = trino_query("SELECT count(*), CAST(sum(amount) AS VARCHAR), count(distinct line_id) FROM lakehouse.readiness.sales_probe")
        assert after == before, after
        report["trino_after_restart"] = after
        report["status"] = "passed"
        print("Spark/Iceberg/MinIO/JDBC/Trino spike passed", flush=True)
    except Exception as exc:
        report["status"] = "failed"
        report["failure"] = str(exc)
        raise
    finally:
        (output / "spike-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        if not args.keep_running:
            # Only our named isolated project; source/user containers and volumes stay untouched.
            compose("stop", "catalog", "minio", "trino", timeout=120)


if __name__ == "__main__":
    main()
