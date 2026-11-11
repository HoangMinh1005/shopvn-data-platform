"""Package boundaries must stay usable without application imports from scripts/."""

import ast
import importlib
from importlib.resources import files
from pathlib import Path

import pytest

from shopvn.common.environment import load_env
from shopvn.config import ProjectPaths
from shopvn.contracts.direct import EXPORTS, PRIMARY_KEYS, SCHEMAS, source_columns
from shopvn.quality.finance import amount_for_day, assert_fin02_matches, queried_fin02

ROOT = Path(__file__).resolve().parents[2]


def test_source_export_and_spark_parser_share_one_contract():
    assert set(EXPORTS) == set(PRIMARY_KEYS) == set(SCHEMAS)
    for table in SCHEMAS:
        columns = source_columns(table)
        assert len(columns) == len(set(columns))
        assert PRIMARY_KEYS[table] in columns
        assert EXPORTS[table] == ",".join(columns)
    assert source_columns("order_items") == (
        "item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_per_item",
        "total_price",
    )


def test_customer_contract_does_not_export_contact_pii():
    assert set(source_columns("customers")) == {"customer_id", "loyalty_tier", "gender", "city"}


def test_baseline_sql_is_a_packaged_resource():
    statement = files("shopvn.ingestion").joinpath("sql/direct_revenue_baseline.sql").read_text()
    assert "GROUP BY o.order_date, o.channel" in statement
    assert "refunded" in statement


def test_process_environment_wins_over_local_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SHOPVN_TEST_CONFIG", "explicit")
    path = tmp_path / ".env"
    path.write_text("# local config\nSHOPVN_TEST_CONFIG=local\nSHOPVN_TEST_NEW=value\n")
    monkeypatch.delenv("SHOPVN_TEST_NEW", raising=False)
    load_env(path)
    import os

    assert os.environ["SHOPVN_TEST_CONFIG"] == "explicit"
    assert os.environ["SHOPVN_TEST_NEW"] == "value"
    monkeypatch.delenv("SHOPVN_TEST_NEW")
    load_env(tmp_path / "missing.env")


def test_runtime_paths_are_configurable_and_reject_escape(monkeypatch, tmp_path):
    monkeypatch.setenv("SHOPVN_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("SHOPVN_RUNTIME_DIR", str(tmp_path / "runtime"))
    paths = ProjectPaths.from_env()
    assert paths.landing == tmp_path / "runtime/direct/landing"
    assert paths.hmac_key == tmp_path / "runtime/direct/hmac.key"
    assert paths.result_file() == tmp_path / "runtime/direct/direct-result.json"
    with pytest.raises(ValueError, match="inside runtime"):
        paths.result_file(tmp_path / "outside.json")


@pytest.mark.parametrize("module", ["shopvn.jobs.direct_finance", "shopvn.jobs.publish_finance"])
def test_job_import_does_not_start_spark_or_parse_cli(module, monkeypatch):
    monkeypatch.setattr("sys.argv", ["pytest", "--unexpected-cli-flag"])
    assert callable(importlib.import_module(module).main)


def test_relative_runtime_is_resolved_against_project_root(monkeypatch, tmp_path):
    monkeypatch.setenv("SHOPVN_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("SHOPVN_RUNTIME_DIR", "custom-runtime")
    assert ProjectPaths.from_env().runtime == tmp_path / "custom-runtime"


def test_application_does_not_import_deployment_or_verification_scripts():
    forbidden = {path.stem for path in (ROOT / "scripts").glob("*.py")}
    for path in (ROOT / "src/shopvn").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            modules = []
            if isinstance(node, ast.Import):
                modules = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            assert not any(name.split(".")[0] in forbidden | {"scripts"} for name in modules), str(
                path
            )


def test_finance_gate_rejects_count_mismatch_even_when_amount_matches():
    actual = queried_fin02([["2026-06-30", "web", 2, "100.50"]])
    assert str(amount_for_day(actual, "2026-06-30")) == "100.50"
    assert amount_for_day(actual, "2026-06-29") == 0
    with pytest.raises(ValueError, match="not replaced"):
        assert_fin02_matches(actual, {("2026-06-30", "web"): (1, actual[("2026-06-30", "web")][1])})
