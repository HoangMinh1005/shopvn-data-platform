"""Small Airflow callables; business transformation remains in the tested Spark module."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from shopvn.clients.trino import trino_query
from shopvn.config import ProjectPaths
from shopvn.ingestion.postgres import extract
from shopvn.orchestration import ops
from shopvn.orchestration.artifacts import load, save
from shopvn.quality.finance import amount_for_day, expected_fin02, queried_fin02


def initialize(**context):
    dag_run = context["dag_run"]
    key = ops.run_key(dag_run.run_id)
    configured = (dag_run.conf or {}).get("business_date") or context["params"].get("business_date")
    clock = context.get("logical_date") or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    business_date = (
        configured
        or (clock.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")) - timedelta(days=1)).date().isoformat()
    )
    datetime.strptime(business_date, "%Y-%m-%d")
    run_type = getattr(dag_run.run_type, "value", dag_run.run_type)
    ops.record_start(key, context["dag"].dag_id, dag_run.run_id, str(run_type), business_date)
    save(
        key,
        "context.json",
        {"run_key": key, "business_date": business_date, "run_id": dag_run.run_id},
    )
    return key


def extract_snapshot(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    state = load(key, "context.json")
    ops.update_run(key, stage="extract", attempts=context["ti"].try_number)
    if "snapshot_id" not in state:
        manifest_path = extract()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        state["snapshot_id"] = manifest["snapshot_id"]
        save(key, "context.json", state)
    else:
        manifest = json.loads(
            (ProjectPaths.from_env().landing / state["snapshot_id"] / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
    period = manifest.get("source_order_date_range") or {
        "min_date": min(r["order_date"] for r in manifest["fin02_expected_by_day_channel"]),
        "max_date": max(r["order_date"] for r in manifest["fin02_expected_by_day_channel"]),
    }
    ops.update_run(
        key,
        snapshot_id=state["snapshot_id"],
        source_min_date=period["min_date"],
        source_max_date=period["max_date"],
    )
    if not period["min_date"] <= state["business_date"] <= period["max_date"]:
        raise ValueError(
            "SourceCoverageError: requested business date is outside the observed source period"
        )
    source_amount = amount_for_day(expected_fin02(manifest), state["business_date"])
    ops.update_run(key, source_amount=source_amount)
    return state["snapshot_id"]


def validate_candidate(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    state = load(key, "context.json")
    ops.update_run(key, stage="validate", attempts=context["ti"].try_number)
    transform = load(key, "transform.json")
    manifest = json.loads(
        (ProjectPaths.from_env().landing / state["snapshot_id"] / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    snapshot = int(transform["candidate_snapshot_id"])
    rows = trino_query(f"""SELECT CAST(order_date AS VARCHAR),channel,eligible_order_count,
                          CAST(net_customer_amount AS VARCHAR) FROM lakehouse.staging.mart_fin02_{key}
                          FOR VERSION AS OF {snapshot} ORDER BY 1,2""")
    actual, expected = queried_fin02(rows), expected_fin02(manifest)
    amount = amount_for_day(actual, state["business_date"])
    source_amount = amount_for_day(expected, state["business_date"])
    injected = (context["dag_run"].conf or {}).get("force_validation_failure", False)
    once = (context["dag_run"].conf or {}).get("validation_fail_once", False) and context[
        "ti"
    ].try_number == 1
    passed = actual == expected and transform["status"] == "passed" and not injected and not once
    save(
        key,
        "validation.json",
        {
            "passed": passed,
            "candidate_snapshot_id": snapshot,
            "source_amount": str(source_amount),
            "candidate_amount": str(amount),
            "row_count": len(rows),
        },
    )
    ops.update_run(
        key,
        candidate_snapshot_id=snapshot,
        candidate_amount=amount,
        amount_delta=amount - source_amount,
        dq_passed=passed,
    )
    if not passed:
        ops.alert(
            key,
            state["business_date"],
            "validation_failed",
            "Candidate reconciliation failed; certified mart publication is blocked",
        )
        raise ValueError("Candidate validation failed; publication blocked")
    return snapshot


def verify_and_finish(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    state = load(key, "context.json")
    approved = load(key, "validation.json")
    publication = trino_query(
        "SELECT DISTINCT publication_id FROM lakehouse.analytics.mart_fin02_daily"
    )
    if publication != [[key]]:
        raise ValueError("Published mart does not match this approved workflow")
    amount = trino_query(
        f"SELECT CAST(sum(net_customer_amount) AS VARCHAR) FROM lakehouse.analytics.mart_fin02_daily WHERE order_date=DATE '{state['business_date']}'"
    )[0][0]
    if Decimal(amount or "0") != Decimal(approved["source_amount"]):
        raise ValueError("Post-publication source-to-Trino amount mismatch")
    ops.finish_success(key)
    save(
        key,
        "complete.json",
        {
            "status": "success",
            "business_date": state["business_date"],
            "fin02": amount,
            "completed_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
        },
    )


def finalize(**context):
    key = ops.run_key(context["dag_run"].run_id)
    with ops.connection() as conn:
        record = conn.execute(
            "SELECT status,business_date FROM ops.pipeline_run WHERE run_key=%s", (key,)
        ).fetchone()
        # Airflow 3.4 keeps archived attempts in task_instance. Only working_set
        # rows represent current task states; a recovered retry must not fail the DAG.
        upstream = conn.execute(
            """SELECT task_id,state FROM task_instance WHERE dag_id=%s AND run_id=%s
               AND working_set=true AND task_id<>'finalize'""",
            (context["dag"].dag_id, context["dag_run"].run_id),
        ).fetchall()
    upstream_succeeded = len(upstream) == 6 and all(task["state"] == "success" for task in upstream)
    if not record or record["status"] != "success" or not upstream_succeeded:
        ops.update_run(key, status="failed", stage="failed")
        ops.alert(
            key,
            record["business_date"] if record else None,
            "pipeline_failed",
            "DAG did not finish validated publication; inspect Airflow task logs",
        )
        raise RuntimeError(
            "Pipeline failed upstream; ALL_DONE finalizer must not turn the DAG green"
        )
