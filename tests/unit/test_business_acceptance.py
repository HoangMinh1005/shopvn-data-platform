"""Golden examples correspond to business rules v1.1; all data is synthetic."""

import json
from copy import deepcopy
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from shopvn.quality.acceptance import (
    allocate_discount,
    deduplicate_events,
    delivery_summary,
    direct_summary,
    marketplace_summary,
    reconstruct_stock,
    require_observed_inventory_period,
)

FIXTURE = json.loads((Path(__file__).parents[1] / "fixtures/business_acceptance.json").read_text())


def test_direct_golden_totals():
    for key, value in direct_summary(FIXTURE["orders"], FIXTURE["lines"], FIXTURE["returns"]).items():
        assert value == FIXTURE["expected"][key], key


@pytest.mark.parametrize("order_id", ["O1", "O8"])
def test_allocation_matches_hand_calculation(order_id):
    order = next(row for row in FIXTURE["orders"] if row["id"] == order_id)
    lines = [row for row in FIXTURE["lines"] if row["order"] == order_id]
    result = allocate_discount(lines, order["discount"])
    assert result == FIXTURE["expected"][f"allocation_{order_id}"]
    assert allocate_discount(list(reversed(lines)), order["discount"]) == result
    assert sum(result.values()) == order["discount"]


def test_zero_quantity_is_excluded_from_allocation():
    assert allocate_discount([{"id": "zero", "quantity": 0, "total_price": 0},
                              {"id": "valid", "quantity": 1, "total_price": 100}], 10) == {"valid": 10}


def test_excess_discount_is_not_clamped_to_zero_revenue():
    with pytest.raises(ValueError):
        allocate_discount([{"id": "x", "quantity": 1, "total_price": 100}], 101)


def test_multiple_refunds_do_not_multiply_order_header():
    order = [row for row in FIXTURE["orders"] if row["id"] == "O2"]
    result = direct_summary(order, [], FIXTURE["returns"])
    assert result["eligible_orders"] == 1
    assert result["merchandise_before_refund"] == 200000
    assert result["refunded"] == 50000


def test_duplicate_return_blocks_reference_calculation():
    with pytest.raises(ValueError):
        direct_summary([], [], [FIXTURE["returns"][0]] * 2)


def test_full_refund_including_shipping_preserves_proxy_limitation():
    orders = [{"id": "FULL", "customer": "C1", "status": "returned", "payment": "refunded",
               "subtotal": 100, "discount": 0, "shipping": 20}]
    refunds = [{"id": "RF", "order": "FULL", "status": "refunded", "amount": 120}]
    result = direct_summary(orders, [], refunds)
    assert result["customer_total_after_refund"] == 0
    assert result["adjusted_merchandise"] == -20  # A proxy, never silently clamped.


def test_marketplace_discount_and_commission_are_not_deducted_twice():
    for key, value in marketplace_summary(FIXTURE["marketplace"]).items():
        assert value == FIXTURE["expected"][key]


def test_marketplace_source_amount_mismatch_is_detected():
    rows = deepcopy(FIXTURE["marketplace"])
    rows[0]["net_revenue"] -= 1
    with pytest.raises(ValueError):
        marketplace_summary(rows)


def test_delivery_denominator_and_eta_day_boundary():
    for key, value in delivery_summary(FIXTURE["shipments"]).items():
        assert value == FIXTURE["expected"][key], key


def test_no_terminal_shipments_has_no_rate():
    assert delivery_summary([{"status": "in_transit"}])["delivery_success_rate"] is None


def test_inventory_reconstruction_uses_signed_changes():
    assert reconstruct_stock(100, [20, -5]) == FIXTURE["expected"]["stock_before_later_changes"]


def test_kafka_replay_is_deduplicated_and_conflict_rejected():
    event = {"event_id": "E1", "data_origin": "simulated", "payload": {"amount": 10}}
    assert deduplicate_events([event, event]) == [event]
    with pytest.raises(ValueError):
        deduplicate_events([event, {**event, "payload": {"amount": 20}}])


def test_same_fixture_three_times_produces_identical_metrics():
    results = [direct_summary(FIXTURE["orders"], FIXTURE["lines"], FIXTURE["returns"]) for _ in range(3)]
    assert results[0] == results[1] == results[2]


def test_approved_finance_primary_uses_customer_total_including_shipping():
    result = direct_summary(FIXTURE["orders"], FIXTURE["lines"], FIXTURE["returns"])
    primary = result[FIXTURE["policy"]["finance_primary_reference_measure"]]
    assert primary == 300002
    assert primary != result["adjusted_merchandise"]


@pytest.mark.parametrize("snapshot_day", [date(2026, 6, 1), date(2026, 6, 30)])
def test_june_inventory_backfill_is_rejected_by_approved_scope(snapshot_day):
    with pytest.raises(ValueError, match="BR32"):
        require_observed_inventory_period(snapshot_day, date(2026, 11, 11))


@pytest.mark.parametrize("snapshot_day", [date(2026, 11, 11), date(2026, 11, 12)])
def test_inventory_period_gate_accepts_go_live_and_later(snapshot_day):
    require_observed_inventory_period(snapshot_day, date(2026, 11, 11))


@pytest.mark.parametrize("amounts,discount,expected", [
    ([547770], "82165.50", {"0": Decimal("82165.50")}),
    ([1, 1], "0.50", {"0": Decimal("0.50"), "1": 0}),
    ([60, 40], "10.50", {"0": Decimal("6.50"), "1": 4}),
])
def test_fractional_source_discount_is_conserved_on_stable_remainder_line(amounts, discount, expected):
    lines = [{"id": str(i), "quantity": 1, "total_price": value} for i, value in enumerate(amounts)]
    result = allocate_discount(lines, Decimal(discount))
    assert result == expected
    assert sum(result.values()) == Decimal(discount)
    assert allocate_discount(list(reversed(lines)), Decimal(discount)) == result
