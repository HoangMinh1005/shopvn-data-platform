from datetime import date, datetime, timezone

import pytest

from shopvn.common.time import utc_instant
from shopvn.contracts.direct import EXPORTS, SCHEMAS
from shopvn.contracts.inventory import InventoryPolicy, eod_publication_missing


@pytest.mark.parametrize(
    "clock,eligible,lag",
    [
        ("2026-11-11T17:00:00+00:00", True, 0),
        ("2026-11-12T00:00:59.999999+07:00", True, 59.999999),
        ("2026-11-11T17:01:00Z", False, 60),
        ("2026-11-12T02:00:00+07:00", False, 7200),
        ("2026-11-11T23:59:59+07:00", False, -1),
    ],
)
def test_eod_window_uses_actual_observation_and_vietnam_midnight(clock, eligible, lag):
    result = InventoryPolicy(date(2026, 11, 11)).classify(
        clock, mode="eod", business_date="2026-11-11"
    )
    assert result["eod_eligible"] is eligible
    assert result["capture_lag_seconds"] == lag
    assert result["snapshot_method"] == (
        "observed_eod_cutoff_window" if eligible else "current_observed"
    )


def test_midday_snapshot_is_not_eod():
    result = InventoryPolicy(date(2026, 11, 11)).classify("2026-11-11T12:00:00+07:00")
    assert not result["eod_eligible"]
    assert result["capture_lag_seconds"] is None


def test_june_backfill_is_blocked_even_when_reading_current_source():
    with pytest.raises(ValueError, match="before go-live"):
        InventoryPolicy(date(2026, 11, 11)).classify(
            "2026-11-12T00:00:10+07:00", mode="eod", business_date="2026-06-30"
        )


def test_current_stock_cannot_be_relabelled_as_yesterday():
    with pytest.raises(ValueError, match="relabelled"):
        InventoryPolicy(date(2026, 11, 11)).classify(
            "2026-11-12T12:00:00+07:00", business_date="2026-11-11"
        )


def test_inventory_configuration_is_required(monkeypatch):
    monkeypatch.delenv("INVENTORY_GO_LIVE_DATE", raising=False)
    with pytest.raises(ValueError, match="Configure"):
        InventoryPolicy.from_env()


def test_nine_table_contract_does_not_export_review_free_text():
    assert len(SCHEMAS) == 9
    assert "comment" not in EXPORTS["product_reviews"].split(",")
    assert {"inventory", "inventory_transactions", "product_reviews"} <= set(SCHEMAS)


def test_naive_observation_clock_is_rejected():
    with pytest.raises(ValueError, match="timezone"):
        utc_instant("2026-11-12T00:00:00")


def test_observation_clock_subclasses_are_normalized_before_date_arithmetic():
    class LibraryDateTime(datetime):
        def astimezone(self, *args, **kwargs):
            raise AssertionError("Do not use library-specific timezone/arithmetic behavior")

    original = LibraryDateTime(2026, 11, 11, 20, tzinfo=timezone.utc)
    normalized = utc_instant(original)
    assert type(normalized) is datetime
    assert normalized.isoformat() == "2026-11-11T20:00:00+00:00"


@pytest.mark.parametrize(
    "clock,last_day,expected",
    [
        ("2026-11-11T10:00:00+07:00", None, False),
        ("2026-11-12T00:09:59+07:00", None, False),
        ("2026-11-12T00:10:00+07:00", None, True),
        ("2026-11-12T00:10:00+07:00", date(2026, 11, 11), False),
        ("2026-11-13T00:10:00+07:00", date(2026, 11, 11), True),
    ],
)
def test_eod_monitor_is_independent_and_does_not_accept_current_only(clock, last_day, expected):
    assert eod_publication_missing(clock, date(2026, 11, 11), last_day) is expected
