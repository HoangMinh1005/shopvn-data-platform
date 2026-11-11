from contextlib import nullcontext
from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace

import pytest

from shopvn.orchestration import artifacts, callbacks, ops, tasks


def test_run_identity_is_stable_and_safe_for_table_suffix():
    assert ops.run_key("run-1") == ops.run_key("run-1")
    assert ops.run_key("run-1") != ops.run_key("run-2")
    assert len(ops.run_key("run-1")) == 24
    assert set(ops.run_key("../run-1")) <= set("abcdef0123456789")


@pytest.mark.parametrize("key", ["../outside", "A" * 24, "0" * 23, "0" * 25])
def test_runtime_path_rejects_invalid_keys(key):
    with pytest.raises(ValueError, match="Invalid run key"):
        artifacts.folder(key)


@pytest.mark.parametrize("field", ["run_id", "dag_run_id"])
def test_callback_accepts_task_and_airflow_deadline_identities(monkeypatch, field):
    result = SimpleNamespace(fetchone=lambda: {"business_date": "2026-06-30"})
    database = SimpleNamespace(execute=lambda *args: result)
    monkeypatch.setattr(ops, "connection", lambda: nullcontext(database))
    assert callbacks._identity({"dag_run": {field: "example"}}) == (
        ops.run_key("example"),
        "2026-06-30",
    )


def test_initialize_uses_enum_value_and_vietnamese_d_minus_one(monkeypatch, tmp_path):
    class RunType(Enum):
        SCHEDULED = "scheduled"

    recorded = []
    monkeypatch.setattr(artifacts, "RUNTIME", tmp_path)
    monkeypatch.setattr(ops, "record_start", lambda *args: recorded.append(args))
    run = SimpleNamespace(run_id="scheduled-example", conf={}, run_type=RunType.SCHEDULED)
    key = tasks.initialize(
        dag_run=run,
        dag=SimpleNamespace(dag_id="shopvn_direct_finance"),
        params={"business_date": ""},
        logical_date=datetime(2026, 11, 12, 20, 0, tzinfo=timezone.utc),
    )
    assert recorded[0][3:] == ("scheduled", "2026-11-12")
    assert artifacts.load(key, "context.json")["business_date"] == "2026-11-12"


@pytest.mark.parametrize("states", [["success"] * 5 + ["failed"], ["success"] * 5])
def test_all_done_cannot_hide_upstream_failure_even_if_ops_says_success(monkeypatch, states):
    query = SimpleNamespace(
        fetchone=lambda: {"status": "success", "business_date": "2026-06-30"},
        fetchall=lambda: [{"state": state} for state in states],
    )
    monkeypatch.setattr(
        ops, "connection", lambda: nullcontext(SimpleNamespace(execute=lambda *a: query))
    )
    monkeypatch.setattr(ops, "update_run", lambda *a, **kw: None)
    monkeypatch.setattr(ops, "alert", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="ALL_DONE finalizer"):
        tasks.finalize(
            dag_run=SimpleNamespace(run_id="test"),
            dag=SimpleNamespace(dag_id="shopvn_direct_finance"),
        )


def test_requested_day_outside_source_coverage_is_blocked(monkeypatch, tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        '{"snapshot_id":"snapshot","source_order_date_range":{"min_date":"2026-06-01","max_date":"2026-06-30"}}'
    )
    monkeypatch.setattr(tasks, "extract", lambda: manifest)
    monkeypatch.setattr(tasks, "load", lambda *a: {"business_date": "2026-11-10"})
    monkeypatch.setattr(tasks, "save", lambda *a: None)
    monkeypatch.setattr(ops, "update_run", lambda *a, **kw: None)
    instance = SimpleNamespace(try_number=1, xcom_pull=lambda **kw: "0" * 24)
    with pytest.raises(ValueError, match="SourceCoverageError"):
        tasks.extract_snapshot(ti=instance)


def test_finalizer_ignores_airflow_archived_retry_attempts(monkeypatch):
    def execute(statement, *args):
        if "task_instance" in statement:
            rows = [{"state": "success"} for _ in range(6)]
            if "working_set=true" not in statement:
                rows.append({"state": "failed"})
            return SimpleNamespace(fetchall=lambda: rows)
        return SimpleNamespace(fetchone=lambda: {"status": "success"})

    monkeypatch.setattr(ops, "connection", lambda: nullcontext(SimpleNamespace(execute=execute)))
    tasks.finalize(
        dag_run=SimpleNamespace(run_id="retry"), dag=SimpleNamespace(dag_id="shopvn_direct_finance")
    )
