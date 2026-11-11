"""Small reference calculations for synthetic acceptance cases, not a production pipeline."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime
from decimal import ROUND_FLOOR, Decimal


def allocate_discount(lines: list[dict], discount: int | Decimal) -> dict[str, int | Decimal]:
    valid = [row for row in lines if row["quantity"] > 0]
    total = sum(row["total_price"] for row in valid)
    if discount < 0 or any(row["total_price"] < 0 for row in valid) or discount > total:
        raise ValueError("Invalid allocation basis")
    if total == 0:
        if discount:
            raise ValueError("No positive allocation basis")
        return {row["id"]: 0 for row in valid}
    if len({row["id"] for row in valid}) != len(valid):
        raise ValueError("Duplicate line identity")
    # Integer VND allocation, stable ties; preserve any source fractional tail on rank 1.
    base = {row["id"]: discount * row["total_price"] // total for row in valid}
    ranking = sorted(valid, key=lambda row: (-(discount * row["total_price"] % total), row["id"]))
    for row in ranking[:int(discount - sum(base.values()))]:
        base[row["id"]] += 1
    fractional_tail = discount - sum(base.values())
    if fractional_tail:
        base[ranking[0]["id"]] += fractional_tail
    return base


def direct_summary(orders: list[dict], lines: list[dict], returns: list[dict]) -> dict:
    refunds: dict[str, int] = defaultdict(int)
    seen_returns = set()
    for row in returns:
        if row["id"] in seen_returns:
            raise ValueError("Duplicate return identity; deduplicate before calculation")
        seen_returns.add(row["id"])
        if row["status"] == "refunded":
            refunds[row["order"]] += row["amount"]
    eligible = [row for row in orders if row["status"] in {"delivered", "returned"}
                and row["payment"] in {"paid", "refunded"}
                and 0 <= row["discount"] <= row["subtotal"]]
    eligible_ids = {row["id"] for row in eligible}
    valid_lines = [row for row in lines if row["order"] in eligible_ids and row["quantity"] > 0]
    merchandise = sum(row["subtotal"] - row["discount"] for row in eligible)
    refunded = sum(refunds[row["id"]] for row in eligible)
    return {
        "eligible_orders": len(eligible),
        "eligible_customers": len({row["customer"] for row in eligible}),
        "merchandise_before_refund": merchandise,
        "refunded": refunded,
        "adjusted_merchandise": merchandise - refunded,
        "customer_total_after_refund": merchandise + sum(row["shipping"] for row in eligible) - refunded,
        "units_sold": sum(row["quantity"] for row in valid_lines),
        "valid_sales_lines": len(valid_lines),
        "invalid_discount_orders": sum(row["discount"] > row["subtotal"] for row in orders),
    }


def marketplace_summary(rows: list[dict]) -> dict:
    merchandise = net = units = 0
    for row in rows:
        expected = (Decimal(row["sale_price"]) * row["quantity"] *
                    (1 - Decimal(row["commission_rate"]))).to_integral_value(rounding=ROUND_FLOOR)
        if expected != row["net_revenue"]:
            raise ValueError("Marketplace source amount mismatch")
        if row["status"] == "completed":
            merchandise += row["sale_price"] * row["quantity"]
            net += row["net_revenue"]
            units += row["quantity"]
    return {"marketplace_merchandise": merchandise, "marketplace_reported_net": net,
            "marketplace_units": units}


def delivery_summary(rows: list[dict]) -> dict:
    terminal = [row for row in rows if row["status"] in {"delivered", "failed", "returned"}]
    delivered = [row for row in rows if row["status"] == "delivered"]
    durations = [(datetime.fromisoformat(row["delivered_at"]) -
                  datetime.fromisoformat(row["shipped_at"])).total_seconds() / 3600
                 for row in delivered]
    if any(duration < 0 for duration in durations):
        raise ValueError("Negative delivery duration")
    return {"delivered": len(delivered), "terminal_shipments": len(terminal),
            "delivery_success_rate": str(Decimal(len(delivered)) / len(terminal)) if terminal else None,
            "average_delivery_hours": str(sum(durations) / len(durations)) if durations else None,
            "late_deliveries": sum(row["delivered_at"][:10] > row["eta"] for row in delivered)}


def deduplicate_events(events: list[dict]) -> list[dict]:
    seen: dict[str, str] = {}
    unique = []
    for event in events:
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
        if event["event_id"] in seen:
            if seen[event["event_id"]] != canonical:
                raise ValueError("Same event_id with conflicting payload")
        else:
            seen[event["event_id"]] = canonical
            unique.append(event)
    return unique


def reconstruct_stock(anchor_quantity: int, later_changes: list[int]) -> int:
    """Synthetic balance arithmetic only; not authorization for BR32-blocked backfill."""
    return anchor_quantity - sum(later_changes)


def require_observed_inventory_period(snapshot_day: date, go_live_day: date) -> None:
    """BR32 reference gate; the deployed pipeline must supply its configured go-live date."""
    if snapshot_day < go_live_day:
        raise ValueError("BR32: inventory snapshot before go-live is outside the approved MVP")
