"""Local service/operational exporter. Prometheus and Grafana read metrics; no outbound notifications."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import urllib.request
from datetime import date, datetime, timezone
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from shopvn.contracts.inventory import eod_publication_missing
from shopvn.orchestration import ops
from shopvn.orchestration.deadline import deadline_missed

HEALTH = Gauge(
    "shopvn_service_up",
    "Service health; batch Spark is represented by job state, not an always-on daemon",
    ["service"],
)
STATE = Gauge("shopvn_pipeline_state", "Latest run state", ["state"])
LAST_FINISH = Gauge(
    "shopvn_pipeline_last_finish_timestamp_seconds", "Latest completed run UTC timestamp"
)
LAST_SUCCESS = Gauge(
    "shopvn_pipeline_last_success_timestamp_seconds", "Latest successful run UTC timestamp"
)
DQ = Gauge("shopvn_pipeline_dq_passed", "Latest reconciliation gate passed")
AMOUNT = Gauge("shopvn_fin02_amount_vnd", "FIN02 for the requested business date", ["layer"])
DELTA = Gauge("shopvn_fin02_delta_vnd", "Candidate minus source for requested business date")
BUSINESS_DAY = Gauge(
    "shopvn_business_date_timestamp_seconds", "Requested business date as UTC midnight"
)
SOURCE_END = Gauge(
    "shopvn_source_end_date_timestamp_seconds", "Observed source business date coverage end"
)
OPEN_ALERTS = Gauge("shopvn_open_alerts", "Persisted unresolved alerts", ["kind"])
DEADLINE = Gauge(
    "shopvn_daily_deadline_missed", "Scheduled publication not completed before 08:00 Vietnam"
)
LAST_POLL = Gauge(
    "shopvn_monitor_last_poll_timestamp_seconds", "Last successful operational DB poll"
)
STATUS = {}
INV_OBSERVED = Gauge(
    "shopvn_inventory_observed_timestamp_seconds", "Latest actual source stock observation"
)
INV_QTY = Gauge(
    "shopvn_inventory_stock_qty", "Current observed total stock; do not sum across days"
)
INV_EOD = Gauge(
    "shopvn_inventory_eod_date_timestamp_seconds",
    "Last actual accepted EOD business date; NaN when none",
)
INV_LAG = Gauge(
    "shopvn_inventory_capture_lag_seconds",
    "Lag from midnight boundary; NaN for current-only capture",
)
INV_FAILED = Gauge("shopvn_inventory_failed", "Latest inventory workflow failed")
INV_MISSING = Gauge(
    "shopvn_inventory_eod_missing", "Yesterday EOD absent after 00:10 VN, only since actual go-live"
)
MKT_FILES = Gauge(
    "shopvn_marketplace_file_count",
    "Latest requested file scope; not all historic publications",
    ["state"],
)
MKT_FAILED = Gauge("shopvn_marketplace_failed", "Latest marketplace workflow failed")
MKT_COMPLETE = Gauge(
    "shopvn_marketplace_complete", "Latest requested all-partner coverage complete"
)
MKT_GATE = Gauge("shopvn_marketplace_gate_passed", "Latest DQ gate; NaN before validation")
MKT_AMOUNT = Gauge(
    "shopvn_marketplace_reported_net_vnd",
    "Candidate reported net; exact reconciliation remains Decimal",
)
MKT_DELTA = Gauge(
    "shopvn_marketplace_reconciliation_delta_vnd",
    "Candidate minus independent source; NaN when unchecked",
)
MKT_DEADLINE = Gauge(
    "shopvn_marketplace_deadline_missed",
    "Current D-1 complete scheduled publication absent by 08:00 VN",
)
MKT_ROWS = Gauge(
    "shopvn_marketplace_row_count", "Active scope accepted/excluded/formula-warning rows", ["kind"]
)
MKT_DATES = Gauge(
    "shopvn_marketplace_business_date_timestamp_seconds",
    "Requested business date encoded as UTC midnight, not processing time",
    ["boundary"],
)
MKT_CAPTURE = Gauge(
    "shopvn_marketplace_capture_completed_timestamp_seconds",
    "Most recent SFTP capture completion; not business date",
)
MKT_FINISH = Gauge(
    "shopvn_marketplace_last_publication_check_timestamp_seconds",
    "Last successful publication check, including partial publications",
)


def http_health(url):
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            payload = response.read()
            return response.status == 200, payload
    except (OSError, ValueError, HTTPException):
        return False, b""


def poll():
    global STATUS
    while True:
        values = {}
        for service, url in {
            "minio": os.getenv("MINIO_HEALTH_URL", "http://minio:9000/minio/health/live"),
            "trino": os.getenv("TRINO_URL", "http://trino:8080") + "/v1/info",
            "airflow_api": os.getenv(
                "AIRFLOW_HEALTH_URL", "http://airflow-api:8080/api/v2/monitor/health"
            ),
        }.items():
            healthy, body = http_health(url)
            HEALTH.labels(service).set(healthy)
            values[service] = healthy
            if service == "airflow_api":
                try:
                    payload = json.loads(body) if healthy else {}
                except (ValueError, TypeError):
                    payload = {}
                for component in ("scheduler", "dag_processor", "metadatabase"):
                    detail = payload.get(component) or {}
                    component_up = detail.get("status") == "healthy"
                    HEALTH.labels("airflow_" + component).set(component_up)
                    values["airflow_" + component] = component_up
        try:
            with psycopg.connect(
                host=os.getenv("SHOPVN_DB_HOST", "host.docker.internal"),
                port=os.getenv("SHOPVN_DB_HOST_PORT", "5434"),
                dbname=os.getenv("SHOPVN_DB_NAME", "shopvn"),
                user=os.environ["SHOPVN_DB_READER_USER"],
                password=os.environ["SHOPVN_DB_READER_PASSWORD"],
                connect_timeout=3,
                options="-c default_transaction_read_only=on",
            ) as source:
                source.execute("SELECT 1")
            HEALTH.labels("source_postgres").set(1)
            values["source_postgres"] = True
        except psycopg.Error:
            HEALTH.labels("source_postgres").set(0)
            values["source_postgres"] = False
        try:
            with socket.create_connection(
                (
                    os.getenv("SHOPVN_SFTP_HOST", "host.docker.internal"),
                    int(os.getenv("SHOPVN_SFTP_HOST_PORT", "2222")),
                ),
                timeout=3,
            ) as sftp_socket:
                sftp_up = sftp_socket.recv(64).startswith(b"SSH-")
        except OSError:
            sftp_up = False
        HEALTH.labels("source_sftp").set(sftp_up)
        values["source_sftp"] = sftp_up
        try:
            with ops.connection() as conn:
                latest = conn.execute(
                    "SELECT * FROM ops.pipeline_run ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                success = conn.execute(
                    "SELECT max(finished_at) t FROM ops.pipeline_run WHERE status='success'"
                ).fetchone()["t"]
                scheduled = conn.execute(
                    """SELECT max(finished_at) t FROM ops.pipeline_run WHERE status='success'
                       AND run_type='scheduled'
                       AND business_date=(now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date-1"""
                ).fetchone()["t"]
                alerts = conn.execute(
                    "SELECT kind,count(*) n FROM ops.alert WHERE resolved_at IS NULL GROUP BY kind"
                ).fetchall()
                alert_runs = conn.execute(
                    "SELECT run_key,kind,count(*) n FROM ops.alert WHERE resolved_at IS NULL GROUP BY run_key,kind"
                ).fetchall()
                inventory = conn.execute(
                    "SELECT * FROM ops.inventory_run ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                inventory_current = conn.execute(
                    "SELECT * FROM ops.inventory_run WHERE current_published=true ORDER BY observed_at DESC,finished_at DESC LIMIT 1"
                ).fetchone()
                last_eod = conn.execute(
                    "SELECT max(business_date) AS last_business_date FROM ops.inventory_run WHERE status='success' AND eod_published=true"
                ).fetchone()["last_business_date"]
                marketplace = conn.execute(
                    "SELECT * FROM ops.marketplace_run ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
                marketplace_finished = conn.execute(
                    "SELECT max(finished_at) t FROM ops.marketplace_run WHERE published=true AND status='success'"
                ).fetchone()["t"]
                marketplace_scheduled = conn.execute("""SELECT max(finished_at) t FROM ops.marketplace_run WHERE status='success'
                    AND run_type='scheduled' AND coverage_complete=true
                    AND end_date=(now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date-1
                    AND start_date=end_date""").fetchone()["t"]
            HEALTH.labels("platform_postgres").set(1)
            values["platform_postgres"] = True
            now = datetime.now(timezone.utc)
            MKT_DEADLINE.set(
                deadline_missed(
                    now, date.fromisoformat(os.environ["MONITOR_START_DATE"]), marketplace_scheduled
                )
            )
            if marketplace:
                for label, column in (("start", "start_date"), ("end", "end_date")):
                    MKT_DATES.labels(label).set(
                        datetime.combine(
                            marketplace[column], datetime.min.time(), timezone.utc
                        ).timestamp()
                    )
                MKT_CAPTURE.set(
                    marketplace["capture_completed_at"].timestamp()
                    if marketplace["capture_completed_at"]
                    else float("nan")
                )
                MKT_FINISH.set(
                    marketplace_finished.timestamp() if marketplace_finished else float("nan")
                )
                for label, column in (
                    ("accepted", "accepted_rows"),
                    ("excluded_status", "excluded_status_rows"),
                    ("formula_warning", "formula_warnings"),
                ):
                    MKT_ROWS.labels(label).set(
                        marketplace[column] if marketplace[column] is not None else float("nan")
                    )
                MKT_FAILED.set(marketplace["status"] == "failed")
                MKT_COMPLETE.set(marketplace["coverage_complete"])
                for label, column in (
                    ("expected", "expected_files"),
                    ("verified", "verified_files"),
                    ("missing", "missing_files"),
                    ("rejected", "rejected_files"),
                ):
                    MKT_FILES.labels(label).set(
                        marketplace[column] if marketplace[column] is not None else float("nan")
                    )
                MKT_GATE.set(
                    float("nan")
                    if marketplace["gate_passed"] is None
                    else marketplace["gate_passed"]
                )
                MKT_AMOUNT.set(
                    float(marketplace["reported_net"])
                    if marketplace["reported_net"] is not None
                    else float("nan")
                )
                MKT_DELTA.set(
                    float(marketplace["reconciliation_delta"])
                    if marketplace["reconciliation_delta"] is not None
                    else float("nan")
                )
            INV_MISSING.set(
                eod_publication_missing(
                    now, date.fromisoformat(os.environ["INVENTORY_GO_LIVE_DATE"]), last_eod
                )
            )
            INV_EOD.set(
                datetime.combine(last_eod, datetime.min.time(), timezone.utc).timestamp()
                if last_eod
                else float("nan")
            )
            if inventory:
                INV_FAILED.set(inventory["status"] == "failed")
                INV_LAG.set(
                    inventory["capture_lag_seconds"]
                    if inventory["capture_lag_seconds"] is not None
                    else float("nan")
                )
            if inventory_current:
                INV_OBSERVED.set(inventory_current["observed_at"].timestamp())
                INV_QTY.set(inventory_current["stock_qty"])
            LAST_POLL.set(now.timestamp())
            DEADLINE.set(
                deadline_missed(
                    now, date.fromisoformat(os.environ["MONITOR_START_DATE"]), scheduled
                )
            )
            if success:
                LAST_SUCCESS.set(success.timestamp())
            if latest:
                for status in ("running", "success", "failed"):
                    STATE.labels(status).set(latest["status"] == status)
                if latest["finished_at"]:
                    LAST_FINISH.set(latest["finished_at"].timestamp())
                DQ.set(bool(latest["dq_passed"]))
                for layer, column in (
                    ("source", "source_amount"),
                    ("candidate", "candidate_amount"),
                ):
                    AMOUNT.labels(layer).set(float(latest[column] or 0))
                DELTA.set(float(latest["amount_delta"] or 0))
                BUSINESS_DAY.set(
                    datetime.combine(
                        latest["business_date"], datetime.min.time(), timezone.utc
                    ).timestamp()
                )
                if latest["source_max_date"]:
                    SOURCE_END.set(
                        datetime.combine(
                            latest["source_max_date"], datetime.min.time(), timezone.utc
                        ).timestamp()
                    )
            OPEN_ALERTS.clear()
            for item in alerts:
                OPEN_ALERTS.labels(item["kind"]).set(item["n"])
            STATUS = {
                "services": values,
                "latest_run": latest,
                "inventory": inventory,
                "inventory_current": inventory_current,
                "last_eod_business_date": last_eod,
                "marketplace": marketplace,
                "open_alerts": alerts,
                "open_alerts_by_run": alert_runs,
                "last_poll": now.isoformat(),
            }
        except Exception as exc:
            HEALTH.labels("platform_postgres").set(0)
            values["platform_postgres"] = False
            STATUS = {"services": values, "monitor_error": type(exc).__name__}
        time.sleep(10)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = generate_latest()
            content_type = CONTENT_TYPE_LATEST
        elif self.path in {"/health", "/api/status"}:
            body = json.dumps(STATUS, default=str).encode()
            content_type = "application/json"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    threading.Thread(target=poll, daemon=True).start()
    ThreadingHTTPServer(
        ("0.0.0.0", int(os.getenv("MONITOR_PORT", "9108"))), Handler
    ).serve_forever()
