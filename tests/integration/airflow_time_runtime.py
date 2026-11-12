"""Exercise actual Airflow/Pendulum clock types without changing DAG runs or source data."""

import json
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

import pendulum

from shopvn.common.time import utc_instant
from shopvn.config import ProjectPaths
from shopvn.orchestration import inventory, ops, tasks


class AirflowClockTests(unittest.TestCase):
    def test_pendulum_clock_is_normalized(self):
        clock = pendulum.parse("2026-11-11T20:00:00Z")
        from datetime import datetime

        self.assertIs(type(utc_instant(clock)), datetime)

    def test_finance_d_minus_one_uses_local_day(self):
        with patch.object(ops, "record_start") as recorded, patch.object(tasks, "save"):
            tasks.initialize(
                dag_run=SimpleNamespace(run_id="clock-test", run_type="scheduled", conf={}),
                dag=SimpleNamespace(dag_id="shopvn_direct_finance"),
                params={},
                logical_date=pendulum.parse("2026-11-11T20:00:00Z"),
            )
        self.assertEqual(recorded.call_args.args[-1], "2026-11-11")

    def test_inventory_cutoff_uses_planned_local_day(self):
        database = nullcontext(SimpleNamespace(execute=lambda *a: None))
        with (
            patch.object(ops, "connection", return_value=database),
            patch.object(inventory, "save") as saved,
        ):
            inventory.begin(
                dag_run=SimpleNamespace(run_id="clock-test", run_type="scheduled", conf={}),
                logical_date=pendulum.parse("2026-11-11T17:00:00Z"),
            )
        self.assertEqual(saved.call_args.args[-1]["business_date"], "2026-11-11")


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(AirflowClockTests)
    )
    output = ProjectPaths.from_env().runtime / "stage1/airflow-clock-tests.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "status": "passed" if result.wasSuccessful() else "failed",
                "tests": result.testsRun,
                "failures": len(result.failures),
                "errors": len(result.errors),
                "scope": "Actual Airflow runtime Pendulum types; no source or metadata writes",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    raise SystemExit(0 if result.wasSuccessful() else 1)
