"""Actual inventory capture is independent of Finance's June source coverage."""

import json
from datetime import timedelta

from shopvn.clients.trino import trino_query
from shopvn.common.time import utc_instant
from shopvn.contracts.inventory import VN, InventoryPolicy
from shopvn.ingestion.postgres import extract
from shopvn.orchestration import ops
from shopvn.orchestration.artifacts import folder, load, save

DAG_ID = "shopvn_inventory_observation"


def begin(**context):
    run = context["dag_run"]
    key = ops.run_key(run.run_id, DAG_ID)
    run_type = getattr(run.run_type, "value", run.run_type)
    mode = "eod" if run_type == "scheduled" else (run.conf or {}).get("mode", "current")
    day = (run.conf or {}).get("business_date") or None
    if run_type == "scheduled":
        clock = context.get("logical_date") or getattr(run, "logical_date", None)
        if clock is None:
            raise ValueError("Scheduled inventory requires the planned logical date")
        # A delayed run must fail its original cutoff, not claim a later day's EOD.
        day = (utc_instant(clock).astimezone(VN).date() - timedelta(days=1)).isoformat()
    save(key, "inventory-context.json", {"run_id": run.run_id, "mode": mode, "business_date": day})
    with ops.connection() as conn:
        conn.execute(
            """INSERT INTO ops.inventory_run(run_key,dag_run_id,mode,status) VALUES(%s,%s,%s,'running')
                     ON CONFLICT(run_key) DO UPDATE SET status='running',finished_at=NULL""",
            (key, run.run_id, mode),
        )
    return key


def capture(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    state = load(key, "inventory-context.json")
    path = folder(key) / "inventory-manifest.json"
    if path.is_file():
        manifest = json.loads(path.read_text(encoding="utf-8"))
    else:
        manifest = json.loads(extract(tables=("inventory", "products")).read_text(encoding="utf-8"))
        save(key, "inventory-manifest.json", manifest)
    classified = InventoryPolicy.from_env().classify(
        manifest["observed_at_utc"], mode=state["mode"], business_date=state["business_date"]
    )
    state.update(
        snapshot_id=manifest["snapshot_id"],
        business_date=classified["business_date"],
        capture=classified,
    )
    save(key, "inventory-context.json", state)
    context["ti"].xcom_push(key="business_date", value=classified["business_date"])
    context["ti"].xcom_push(key="mode", value=state["mode"])
    with ops.connection() as conn:
        conn.execute(
            """UPDATE ops.inventory_run SET snapshot_id=%s,business_date=%s,observed_at=%s,
                     cutoff_at=%s,capture_lag_seconds=%s,eod_eligible=%s WHERE run_key=%s""",
            (
                manifest["snapshot_id"],
                classified["business_date"],
                manifest["observed_at_utc"],
                classified["cutoff_at"],
                classified["capture_lag_seconds"],
                classified["eod_eligible"],
                key,
            ),
        )
    return manifest["snapshot_id"]


def verify(**context):
    key = context["ti"].xcom_pull(task_ids="initialize")
    result = load(key, "inventory-result.json")
    state = load(key, "inventory-context.json")
    if result["status"] != "passed" or not result["warehouse_reconciliation"]:
        raise ValueError("Inventory pipeline did not pass source reconciliation")
    target = (
        "lakehouse.analytics.current_stock"
        if state["mode"] == "current"
        else "lakehouse.gold.fact_inventory_daily_snapshot"
    )
    where = (
        ""
        if state["mode"] == "current"
        else f" WHERE snapshot_date=DATE '{state['business_date']}'"
    )
    values = trino_query(f"SELECT count(*),sum(stock_qty) FROM {target}{where}")[0]
    if values != [result["rows"], result["stock_qty"]]:
        raise ValueError("Inventory post-publication Trino reconciliation failed")
    with ops.connection() as conn:
        conn.execute(
            """UPDATE ops.inventory_run SET status='success',rows=%s,stock_qty=%s,
                     current_published=%s,eod_published=%s,finished_at=now() WHERE run_key=%s""",
            (
                result["rows"],
                result["stock_qty"],
                result["current_published"],
                result["eod_published"],
                key,
            ),
        )
        conn.execute(
            "UPDATE ops.alert SET resolved_at=now(),resolved_by_run_key=%s WHERE kind='inventory_failed' AND resolved_at IS NULL AND business_date=%s",
            (key, state["business_date"]),
        )


def finalize(**context):
    key = ops.run_key(context["dag_run"].run_id, DAG_ID)
    try:
        partial = load(key, "inventory-result.json")
    except (FileNotFoundError, json.JSONDecodeError):
        partial = {}
    with ops.connection() as conn:
        if "current_published" in partial:
            conn.execute(
                "UPDATE ops.inventory_run SET rows=%s,stock_qty=%s,current_published=%s,eod_published=%s WHERE run_key=%s",
                (
                    partial.get("rows"),
                    partial.get("stock_qty"),
                    partial["current_published"],
                    partial.get("eod_published", False),
                    key,
                ),
            )
        row = conn.execute(
            "SELECT status,business_date FROM ops.inventory_run WHERE run_key=%s", (key,)
        ).fetchone()
        tasks = conn.execute(
            "SELECT state FROM task_instance WHERE dag_id=%s AND run_id=%s AND working_set=true AND task_id<>'finalize'",
            (DAG_ID, context["dag_run"].run_id),
        ).fetchall()
        good = (
            row
            and row["status"] == "success"
            and len(tasks) == 4
            and all(task["state"] == "success" for task in tasks)
        )
        if not good:
            conn.execute(
                "UPDATE ops.inventory_run SET status='failed',finished_at=now() WHERE run_key=%s",
                (key,),
            )
    if not good:
        ops.alert(
            key,
            row["business_date"] if row else None,
            "inventory_failed",
            "Inventory observation failed; late captures must not be labelled EOD",
        )
        raise RuntimeError("Inventory upstream failure")
