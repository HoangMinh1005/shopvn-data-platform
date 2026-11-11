"""Local persisted alert callbacks; intentionally no email/Slack sends."""

from shopvn.orchestration import ops
from shopvn.orchestration.deadline import runtime_deadline_missed


def _identity(context):
    run = context.get("dag_run")
    run_id = (
        (run.get("run_id") or run.get("dag_run_id"))
        if isinstance(run, dict)
        else getattr(run, "run_id", None)
    )
    if not run_id:
        return None, None
    key = ops.run_key(run_id)
    with ops.connection() as conn:
        row = conn.execute(
            "SELECT business_date FROM ops.pipeline_run WHERE run_key=%s", (key,)
        ).fetchone()
    return key, row["business_date"] if row else None


def task_failed(context):
    key, day = _identity(context)
    if key:
        ti = context.get("task_instance") or context.get("ti")
        ops.update_run(key, status="failed", error_class=type(context.get("exception")).__name__)
        ops.alert(
            key,
            day,
            "pipeline_failed",
            f"Task {getattr(ti, 'task_id', 'unknown')} failed; see task logs",
        )


def task_retry(context):
    key, day = _identity(context)
    if key:
        ti = context.get("task_instance") or context.get("ti")
        ops.alert(
            key,
            day,
            "task_retry",
            f"Retrying task {getattr(ti, 'task_id', 'unknown')}",
            "warning",
            str(getattr(ti, "try_number", 0)),
        )


def deadline_exceeded(**kwargs):
    context = kwargs.get("context", {})
    key, day = _identity(context)
    if key:
        deadline_time = (context.get("deadline") or {}).get("deadline_time")
        if deadline_time:
            with ops.connection() as conn:
                row = conn.execute(
                    "SELECT finished_at FROM ops.pipeline_run WHERE run_key=%s", (key,)
                ).fetchone()
            if row and not runtime_deadline_missed(row["finished_at"], deadline_time):
                return
        ops.alert(key, day, "deadline_runtime", "DAG exceeded the 30-minute runtime deadline")
