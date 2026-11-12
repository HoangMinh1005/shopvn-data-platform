import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from shopvn.orchestration import inventory, ops


def test_inventory_and_finance_run_keys_do_not_collide():
    assert ops.run_key("same") != ops.run_key("same", inventory.DAG_ID)


@pytest.mark.parametrize(
    "run_type,conf,expected",
    [("scheduled", {}, "eod"), ("manual", {}, "current"), ("manual", {"mode": "eod"}, "eod")],
)
def test_capture_mode_is_explicit_and_scheduled_runs_are_eod(monkeypatch, run_type, conf, expected):
    saved = []
    monkeypatch.setattr(inventory, "save", lambda key, name, value: saved.append(value))
    monkeypatch.setattr(
        ops, "connection", lambda: nullcontext(SimpleNamespace(execute=lambda *a: None))
    )
    inventory.begin(
        dag_run=SimpleNamespace(run_id="test", run_type=run_type, conf=conf),
        logical_date="2026-11-11T17:00:00Z",
    )
    assert saved[0]["mode"] == expected
    if run_type == "scheduled":
        assert saved[0]["business_date"] == "2026-11-11"


def test_scheduled_inventory_requires_planned_cutoff():
    with pytest.raises(ValueError, match="planned logical date"):
        inventory.begin(dag_run=SimpleNamespace(run_id="late", run_type="scheduled", conf={}))


def test_delayed_scheduled_inventory_cannot_claim_a_later_days_eod(monkeypatch, tmp_path):
    monkeypatch.setenv("INVENTORY_GO_LIVE_DATE", "2026-11-11")
    saved = {}
    monkeypatch.setattr(inventory, "save", lambda key, name, value: saved.update({name: value}))
    monkeypatch.setattr(
        ops, "connection", lambda: nullcontext(SimpleNamespace(execute=lambda *a: None))
    )
    inventory.begin(
        dag_run=SimpleNamespace(run_id="late", run_type="scheduled", conf={}),
        logical_date="2026-11-11T17:00:00Z",
    )
    manifest = {"snapshot_id": "0" * 32, "observed_at_utc": "2026-11-12T17:00:10Z"}
    (tmp_path / "inventory-manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(inventory, "folder", lambda key: tmp_path)
    monkeypatch.setattr(inventory, "load", lambda key, name: saved[name])
    inventory.capture(ti=SimpleNamespace(xcom_pull=lambda **kw: "key", xcom_push=lambda **kw: None))
    capture = saved["inventory-context.json"]["capture"]
    assert capture["business_date"] == "2026-11-11"
    assert capture["capture_lag_seconds"] == 86410
    assert not capture["eod_eligible"]


def test_capture_retry_reuses_original_observation_and_publishes_parameters(monkeypatch, tmp_path):
    monkeypatch.setenv("INVENTORY_GO_LIVE_DATE", "2026-11-11")
    manifest = {"snapshot_id": "0" * 32, "observed_at_utc": "2026-11-11T17:00:10Z"}
    (tmp_path / "inventory-manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(inventory, "folder", lambda key: tmp_path)
    monkeypatch.setattr(
        inventory, "load", lambda *a: {"mode": "eod", "business_date": "2026-11-11"}
    )
    monkeypatch.setattr(inventory, "save", lambda *a: None)
    monkeypatch.setattr(
        inventory, "extract", lambda **kw: pytest.fail("Retry must not replace the source capture")
    )
    monkeypatch.setattr(
        ops, "connection", lambda: nullcontext(SimpleNamespace(execute=lambda *a: None))
    )
    pushed = {}
    ti = SimpleNamespace(
        xcom_pull=lambda **kw: "key", xcom_push=lambda key, value: pushed.update({key: value})
    )
    assert inventory.capture(ti=ti) == "0" * 32
    assert pushed == {"mode": "eod", "business_date": "2026-11-11"}


def test_inventory_all_done_cannot_hide_an_upstream_failure(monkeypatch):
    monkeypatch.setattr(inventory, "load", lambda *a: {})
    query = SimpleNamespace(
        fetchone=lambda: {"status": "success", "business_date": "2026-11-11"},
        fetchall=lambda: [{"state": "success"}] * 3 + [{"state": "failed"}],
    )
    monkeypatch.setattr(
        ops, "connection", lambda: nullcontext(SimpleNamespace(execute=lambda *a: query))
    )
    monkeypatch.setattr(ops, "alert", lambda *a: None)
    with pytest.raises(RuntimeError, match="upstream"):
        inventory.finalize(dag_run=SimpleNamespace(run_id="bad"))
