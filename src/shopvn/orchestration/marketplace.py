"""Airflow adapters: immutable capture, exact candidate gate, truthful partial coverage."""

import json
from datetime import datetime, timedelta

from shopvn.clients.trino import trino_query
from shopvn.common.time import utc_instant
from shopvn.config import ProjectPaths
from shopvn.contracts.inventory import VN
from shopvn.contracts.marketplace import PARTNERS, selection
from shopvn.ingestion.sftp import extract
from shopvn.orchestration import ops
from shopvn.orchestration.artifacts import load, save
from shopvn.quality.marketplace import run_folder, validate

DAG_ID = "shopvn_marketplace_finance"


def begin(**context):
    run = context["dag_run"]
    key = ops.run_key(run.run_id, DAG_ID)
    kind = getattr(run.run_type, "value", run.run_type)
    conf = run.conf or {}
    logical = context.get("logical_date") or datetime.now(VN)
    day = (utc_instant(logical).astimezone(VN).date() - timedelta(days=1)).isoformat()
    start, end = (
        conf.get("start_date") or day,
        conf.get("end_date") or conf.get("start_date") or day,
    )
    partners = conf.get("partners") or list(PARTNERS)
    selection(start, end, partners)
    state = {
        "run_id": run.run_id,
        "run_type": kind,
        "start": start,
        "end": end,
        "partners": partners,
    }
    save(key, "marketplace-context.json", state)
    with ops.connection() as conn:
        conn.execute(
            """INSERT INTO ops.marketplace_run(run_key,dag_run_id,run_type,start_date,end_date,status)
                        VALUES(%s,%s,%s,%s,%s,'running') ON CONFLICT(run_key) DO UPDATE SET status='running',finished_at=NULL""",
            (key, run.run_id, kind, start, end),
        )
    return key


def capture(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    state = load(key, "marketplace-context.json")
    if "capture_id" in state:
        path = (
            ProjectPaths.from_env().runtime
            / "marketplace/landing"
            / state["capture_id"]
            / "manifest.json"
        )
    else:
        path = extract(
            state["start"],
            state["end"],
            state["partners"],
            wait_seconds=30 if state["run_type"] == "scheduled" else 0,
        )
        state["capture_id"] = path.parent.name
        save(key, "marketplace-context.json", state)
    manifest = json.loads(path.read_text())
    with ops.connection() as conn:
        conn.execute(
            "UPDATE ops.marketplace_run SET capture_completed_at=%s WHERE run_key=%s",
            (manifest["completed_at"], key),
        )
    verified = manifest["states"].get("verified", 0)
    missing = sum(manifest["states"].get(s, 0) for s in ("missing", "checksum_missing"))
    rejected = len(manifest["files"]) - verified - missing
    complete = verified == len(manifest["files"]) and set(state["partners"]) == set(PARTNERS)
    with ops.connection() as conn:
        conn.execute(
            """UPDATE ops.marketplace_run SET capture_id=%s,expected_files=%s,verified_files=%s,
                      missing_files=%s,rejected_files=%s,coverage_complete=%s WHERE run_key=%s""",
            (
                state["capture_id"],
                len(manifest["files"]),
                verified,
                missing,
                rejected,
                complete,
                key,
            ),
        )
        for entry in manifest["files"]:
            reference = entry.get("reference", {})
            conn.execute(
                """INSERT INTO ops.marketplace_file VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(run_key,partner,business_date) DO UPDATE SET state=EXCLUDED.state""",
                (
                    key,
                    entry["partner"],
                    entry["business_date"],
                    entry["filename"],
                    entry["state"],
                    entry.get("sha256"),
                    entry.get("bytes"),
                    reference.get("rows"),
                    reference.get("accepted"),
                ),
            )
    if missing or rejected:
        ops.alert(
            key,
            state["end"],
            "marketplace_incomplete",
            "Marketplace coverage incomplete; missing/rejected files must not become zero revenue",
            severity="warning",
        )
    if not verified:
        raise ValueError("No verified marketplace files in requested source period")
    return state["capture_id"]


def check(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    conf = context["dag_run"].conf or {}
    fail = conf.get("force_validation_failure", False) or (
        conf.get("validation_fail_once", False) and context["ti"].try_number == 1
    )
    try:
        result = validate(key, force_failure=fail)
    except Exception:
        try:
            result = json.loads((run_folder(key) / "validation.json").read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            result = {}
        with ops.connection() as conn:
            conn.execute(
                "UPDATE ops.marketplace_run SET gate_passed=false,reconciliation_delta=%s WHERE run_key=%s",
                (result.get("reconciliation_delta"), key),
            )
        raise
    with ops.connection() as conn:
        conn.execute(
            "UPDATE ops.marketplace_run SET gate_passed=true,reported_net=%s,reconciliation_delta=%s WHERE run_key=%s",
            (result["reported_net_total"], result["reconciliation_delta"], key),
        )


def verify(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    if trino_query(
        "SELECT DISTINCT publication_id FROM lakehouse.analytics.mart_commerce_finance_daily"
    ) != [[key]]:
        raise ValueError("Marketplace certified publication does not match this workflow")
    report = json.loads((run_folder(key) / "candidate.json").read_text())
    approved = json.loads((run_folder(key) / "validation.json").read_text())
    amount = trino_query(
        "SELECT CAST(sum(partner_reported_net) AS VARCHAR) FROM lakehouse.analytics.mart_commerce_finance_daily WHERE report_type='sales'"
    )[0][0]
    from decimal import Decimal

    if Decimal(amount or "0") != Decimal(approved["reported_net_total"]):
        raise ValueError("Post-publication marketplace reconciliation failed")
    verified_scopes = {
        (r[0], r[1])
        for r in trino_query(
            "SELECT channel,CAST(business_date AS VARCHAR) FROM lakehouse.analytics.mart_commerce_finance_daily WHERE report_type='sales' AND coverage_status='verified'"
        )
    }
    with ops.connection() as conn:
        conn.execute(
            "UPDATE ops.marketplace_run SET status='success',published=true,accepted_rows=%s,formula_warnings=%s,excluded_status_rows=%s,finished_at=now() WHERE run_key=%s",
            (
                report["accepted_rows"],
                report["formula_warnings"],
                report["excluded_status_rows"],
                key,
            ),
        )
        conn.execute(
            """UPDATE ops.alert SET resolved_at=now(),resolved_by_run_key=%s WHERE kind='marketplace_failed'
                        AND resolved_at IS NULL AND business_date=(SELECT end_date FROM ops.marketplace_run WHERE run_key=%s)""",
            (key, key),
        )
        unresolved = conn.execute("""SELECT a.alert_key,f.partner,f.business_date FROM ops.alert a
            JOIN ops.marketplace_file f ON f.run_key=a.run_key WHERE a.kind='marketplace_incomplete'
            AND a.resolved_at IS NULL AND f.state<>'verified'""").fetchall()
        scopes = {}
        for row in unresolved:
            scopes.setdefault(row["alert_key"], set()).add(
                (row["partner"], str(row["business_date"]))
            )
        for alert_key, affected in scopes.items():
            if affected <= verified_scopes:
                conn.execute(
                    "UPDATE ops.alert SET resolved_at=now(),resolved_by_run_key=%s WHERE alert_key=%s",
                    (key, alert_key),
                )


def finalize(**context):
    key = ops.run_key(context["dag_run"].run_id, DAG_ID)
    with ops.connection() as conn:
        row = conn.execute(
            "SELECT status,end_date FROM ops.marketplace_run WHERE run_key=%s", (key,)
        ).fetchone()
        tasks = conn.execute(
            "SELECT state FROM task_instance WHERE dag_id=%s AND run_id=%s AND working_set=true AND task_id<>'finalize'",
            (DAG_ID, context["dag_run"].run_id),
        ).fetchall()
        good = (
            row
            and row["status"] == "success"
            and len(tasks) == 6
            and all(t["state"] == "success" for t in tasks)
        )
        if not good:
            conn.execute(
                "UPDATE ops.marketplace_run SET status='failed',finished_at=now() WHERE run_key=%s",
                (key,),
            )
    if not good:
        ops.alert(
            key,
            row["end_date"] if row else None,
            "marketplace_failed",
            "Marketplace workflow failed; certified output must remain gated",
        )
        raise RuntimeError("Marketplace upstream failure")
