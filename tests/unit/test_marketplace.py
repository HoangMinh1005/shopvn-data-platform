import csv
import hashlib
import io
import json
from decimal import Decimal

import pytest

from shopvn.config import ProjectPaths
from shopvn.contracts.marketplace import COLUMNS, decimal_value, parse_row, selection
from shopvn.ingestion.sftp import capture_file, connection, extract, parse_file


def row(**changes):
    return {
        **dict(
            zip(
                COLUMNS,
                (
                    "O1",
                    "1",
                    "SKU1",
                    "lazada",
                    "2026-06-01",
                    "2",
                    "100000",
                    "5000",
                    "0.08",
                    "184000",
                    "2026-06-16",
                    "completed",
                ),
            )
        ),
        **changes,
    }


def csv_bytes(rows, columns=COLUMNS):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


class FakeSFTP:
    def __init__(self, files):
        self.files = files

    def open(self, path, mode):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def stat(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return object()


def remote_files(payload, checksum=None):
    name = "/marketplace/incoming/lazada/lazada_20260601.csv"
    return {name: payload, name + ".md5": (checksum or hashlib.md5(payload).hexdigest()).encode()}


def test_marketplace_primary_amount_basis():
    _, _, merchandise, net, delta = parse_row(row(), "lazada", "2026-06-01")
    assert (merchandise, net, delta) == (Decimal(200000), Decimal(184000), 0)


@pytest.mark.parametrize(
    "changes",
    [
        {"partner": "shopee"},
        {"order_date": "2026-06-02"},
        {"quantity_sold": "0"},
        {"quantity_sold": "1.5"},
        {"shopvn_product_id": ""},
        {"status": "unknown"},
        {"sale_price": "NaN"},
        {"sale_price": "100.001"},
        {"net_revenue": "180000"},
        {"commission_rate": "1.1"},
        {"settlement_date": "2026-06-17"},
    ],
)
def test_invalid_marketplace_payloads_are_rejected(changes):
    with pytest.raises(ValueError):
        parse_row(row(**changes), "lazada", "2026-06-01")


@pytest.mark.parametrize("value", ["Infinity", "-Infinity", "nan", "1000000000000000000", "0.001"])
def test_money_is_not_silently_truncated(value):
    with pytest.raises(ValueError):
        decimal_value(value)


def test_new_extra_column_is_ignored_but_required_schema_is_enforced(tmp_path):
    path = tmp_path / "input.csv"
    path.write_bytes(csv_bytes([{**row(), "new_field": "future"}], COLUMNS + ("new_field",)))
    result = parse_file(path, "lazada", "2026-06-01")
    assert result["ignored_columns"] == ["new_field"]
    assert result["accepted"] == 1
    path.write_bytes(
        csv_bytes(
            [{k: v for k, v in row().items() if k != "net_revenue"}],
            tuple(c for c in COLUMNS if c != "net_revenue"),
        )
    )
    with pytest.raises(ValueError, match="required CSV"):
        parse_file(path, "lazada", "2026-06-01")


def test_duplicate_and_conflict_have_reconciliation_and_quarantine(tmp_path):
    path = tmp_path / "input.csv"
    path.write_bytes(csv_bytes([row(), row(), row(net_revenue="184001")]))
    result = parse_file(path, "lazada", "2026-06-01")
    assert result["rows"] == result["accepted"] + result["rejected"] == 3
    assert result["duplicates"] == 1 and result["blocking_errors"] == 1
    assert len(path.with_suffix(".quarantine.jsonl").read_text().splitlines()) == 2


def test_missing_and_bad_checksum_never_reach_parser(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "shopvn.ingestion.sftp.parse_file", lambda *a: pytest.fail("Do not parse unverified bytes")
    )
    assert capture_file(FakeSFTP({}), tmp_path, "lazada", "2026-06-01")["state"] == "missing"
    payload = csv_bytes([row()])
    assert (
        capture_file(FakeSFTP(remote_files(payload, "0" * 32)), tmp_path, "lazada", "2026-06-01")[
            "state"
        ]
        == "checksum_mismatch"
    )
    files = remote_files(payload)
    files.pop(next(k for k in files if k.endswith(".md5")))
    assert (
        capture_file(FakeSFTP(files), tmp_path, "lazada", "2026-06-01")["state"]
        == "checksum_missing"
    )


def test_extract_immutable_complete_manifest_and_ignored_runtime(tmp_path):
    paths = ProjectPaths(tmp_path, tmp_path / "runtime")
    manifest = json.loads(
        extract(
            "2026-06-01",
            "2026-06-01",
            ("lazada",),
            paths=paths,
            client=FakeSFTP(remote_files(csv_bytes([row()]))),
        ).read_text()
    )
    assert manifest["states"] == {"verified": 1}
    assert manifest["files"][0]["reference"]["reported_net"] == "184000"


def test_host_pin_is_required_before_network_authentication(monkeypatch):
    monkeypatch.delenv("SHOPVN_SFTP_HOST_KEY_SHA256", raising=False)
    with pytest.raises(ValueError, match="Pin"):
        with connection():
            pytest.fail("No unpinned connection")


@pytest.mark.parametrize(
    "start,end,partners",
    [
        ("2026-06-02", "2026-06-01", ("lazada",)),
        ("2026-06-01", "2026-06-01", ("../source",)),
        ("2026-06-01", "2026-06-01", ("lazada", "lazada")),
    ],
)
def test_scope_is_bounded_and_whitelisted(start, end, partners):
    with pytest.raises(ValueError):
        selection(start, end, partners)


def test_float_source_rounding_warning_preserves_reported_net():
    _, _, merchandise, net, delta = parse_row(
        row(sale_price="1000", quantity_sold="1", commission_rate="0.07", net_revenue="929"),
        "lazada",
        "2026-06-01",
    )
    assert merchandise == 1000 and net == 929 and delta == -1


def test_late_file_arrival_is_bounded_and_does_not_require_source_mutation(tmp_path, monkeypatch):
    client = FakeSFTP({})
    clock = [0]

    def sleep(seconds):
        clock[0] += seconds
        client.files.update(remote_files(csv_bytes([row()])))

    monkeypatch.setattr("shopvn.ingestion.sftp.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("shopvn.ingestion.sftp.time.sleep", sleep)
    result = json.loads(
        extract(
            "2026-06-01",
            "2026-06-01",
            ("lazada",),
            paths=ProjectPaths(tmp_path, tmp_path / "runtime"),
            client=client,
            wait_seconds=5,
        ).read_text()
    )
    assert result["states"] == {"verified": 1}
    assert clock[0] == 5


def test_invalid_schema_file_is_blocked_with_row_quarantine(tmp_path):
    result = capture_file(
        FakeSFTP(remote_files(csv_bytes([row(), row(external_order_id="bad", status="bad")]))),
        tmp_path,
        "lazada",
        "2026-06-01",
    )
    assert result["state"] == "invalid_schema_or_payload"
    assert result["reference"]["rows"] == 2
    assert result["reference"]["accepted"] + result["reference"]["rejected"] == 2


def test_mismatched_host_key_refuses_password_auth(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("SHOPVN_SFTP_HOST_KEY_SHA256", "SHA256:untrusted")
    monkeypatch.setattr("shopvn.ingestion.sftp.socket.create_connection", lambda *a, **kw: object())
    transport = SimpleNamespace(
        start_client=lambda **kw: None,
        close=lambda: None,
        get_remote_server_key=lambda: SimpleNamespace(asbytes=lambda: b"new-key"),
        auth_password=lambda *a: pytest.fail("Password must not be sent to untrusted host"),
    )
    monkeypatch.setattr("shopvn.ingestion.sftp.paramiko.Transport", lambda *a: transport)
    with pytest.raises(ValueError, match="host key mismatch"):
        with connection():
            pytest.fail("Untrusted server")


@pytest.mark.parametrize(
    "fault", [None, "aov", "net", "fact_count", "direct_count", "settlement_amount", "missing_zero"]
)
def test_exact_candidate_validation_uses_independent_counts_amounts_and_basis(
    tmp_path, monkeypatch, fault
):
    from shopvn.ingestion import postgres
    from shopvn.quality import marketplace as quality

    paths = ProjectPaths(tmp_path, tmp_path / "runtime")
    monkeypatch.setattr(ProjectPaths, "from_env", lambda: paths)
    key = "a" * 24
    root = paths.runtime / "marketplace/runs" / key
    root.mkdir(parents=True)
    candidate = {
        "status": "passed",
        "tables": {
            "mart": {"name": "shopvn.staging.fixture_mart", "snapshot_id": 1},
            "facts": {"name": "shopvn.staging.fixture_facts", "snapshot_id": 2},
        },
        "expected": [
            {
                "partner": "lazada",
                "business_date": "2026-06-01",
                "accepted": 1,
                "merchandise": "200",
                "reported_net": "184",
                "units": 2,
                "eligible_orders": 1,
            }
        ],
    }
    (root / "candidate.json").write_text(json.dumps(candidate))

    def query(statement):
        if "substr(source_system" in statement:
            return [["lazada", 20260601, 2 if fault == "fact_count" else 1, "200", "184", 2]]
        if "report_type='scheduled_settlement'" in statement:
            return [
                ["2026-06-16", "lazada", "183" if fault == "settlement_amount" else "184", None]
            ]
        if "channel LIKE 'direct_%'" in statement:
            return [["2026-06-01", "direct_web", 1, "50", "65"]]
        result = [
            [
                "2026-06-01",
                "lazada",
                "200",
                "183" if fault == "net" else "184",
                1,
                2,
                "184" if fault == "aov" else "200",
            ]
        ]
        if fault == "missing_zero":
            result.append(["2026-06-01", "shopee", "0", "0", 0, 0, None])
        return result

    monkeypatch.setattr(quality, "trino_query", query)
    monkeypatch.setattr(
        postgres,
        "finance_reference",
        lambda: [
            {
                "order_date": "2026-06-01",
                "channel": "web",
                "eligible_order_count": 2 if fault == "direct_count" else 1,
                "merchandise_before_refund": Decimal(50),
                "net_customer_amount": Decimal(65),
            }
        ],
    )
    if fault:
        with pytest.raises(ValueError):
            quality.validate(key)
        assert not json.loads((root / "validation.json").read_text())["passed"]
    else:
        assert quality.validate(key)["passed"]


def test_interrupted_download_never_creates_verified_file_or_manifest(tmp_path):
    payload = csv_bytes([row()])

    class Broken(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise OSError("injected transfer interruption")
            return super().read(size)

    class Interrupted(FakeSFTP):
        def open(self, path, mode):
            if path.endswith(".md5"):
                return super().open(path, mode)
            return Broken(payload)

    paths = ProjectPaths(tmp_path, tmp_path / "runtime")
    with pytest.raises(OSError, match="interruption"):
        extract(
            "2026-06-01",
            "2026-06-01",
            ("lazada",),
            paths=paths,
            client=Interrupted(remote_files(payload)),
        )
    assert list(paths.runtime.rglob("*.partial"))
    assert not list(paths.runtime.rglob("manifest.json"))
    assert not list(paths.runtime.rglob("*.csv"))


def test_conflicting_valid_payloads_are_blocked_not_arbitrarily_selected(tmp_path):
    path = tmp_path / "conflict.csv"
    path.write_bytes(csv_bytes([row(), row(sale_price="90000", net_revenue="165600")]))
    result = parse_file(path, "lazada", "2026-06-01")
    assert result["blocking_errors"] == 1 and result["accepted"] == 1 and result["rejected"] == 1
    assert "Conflicting duplicate identity" in path.with_suffix(".quarantine.jsonl").read_text()
