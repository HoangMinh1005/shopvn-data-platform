"""Marketplace file contract v1. Business rule remains 1.1; no bank-cash inference."""

import hashlib
import json
import re
from datetime import date, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation

PARTNER_CHANNEL_KEYS = {"lazada": 3, "shopee": 4, "tiktok": 5}
PARTNERS = tuple(PARTNER_CHANNEL_KEYS)
COLUMNS = (
    "external_order_id",
    "shopvn_product_id",
    "seller_sku",
    "partner",
    "order_date",
    "quantity_sold",
    "sale_price",
    "platform_discount",
    "commission_rate",
    "net_revenue",
    "settlement_date",
    "status",
)
SCHEMA = "external_order_id string,shopvn_product_id int,seller_sku string,partner string,order_date date,quantity_sold long,sale_price decimal(20,2),platform_discount decimal(20,2),commission_rate decimal(10,6),net_revenue decimal(20,2),settlement_date date,status string"


def selection(start, end, partners=PARTNERS):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    partners = tuple(partners)
    if not partners or len(set(partners)) != len(partners) or not set(partners) <= set(PARTNERS):
        raise ValueError("Unknown/duplicate marketplace partner")
    if start > end or (end - start).days > 366:
        raise ValueError("Invalid or unbounded marketplace date range")
    return [
        (p, (start + timedelta(days=n)).isoformat())
        for p in sorted(partners)
        for n in range((end - start).days + 1)
    ]


def file_name(partner, day):
    selection(day, day, (partner,))
    return f"{partner}_{date.fromisoformat(day):%Y%m%d}.csv"


def decimal_value(value, precision=20, scale=2):
    try:
        result = Decimal(value)
        if not result.is_finite() or result != result.quantize(Decimal(1).scaleb(-scale)):
            raise ValueError("Invalid numeric precision")
        if abs(result) >= Decimal(10) ** (precision - scale):
            raise ValueError("Numeric overflow")
        return result
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Invalid Decimal") from exc


def parse_row(row, partner, day):
    if any(row.get(c) is None or not str(row[c]).strip() for c in COLUMNS):
        raise ValueError("Missing required value")
    if None in row:
        raise ValueError("Malformed CSV row width")
    if row["partner"] != partner or row["order_date"] != day:
        raise ValueError("Partner/business-date mismatch")
    for name in ("shopvn_product_id", "quantity_sold"):
        if not re.fullmatch(r"[1-9]\d*", row[name]) or int(row[name]) > 2147483647:
            raise ValueError("Invalid positive integer")
    if row["status"] not in {"completed", "returned", "disputed"}:
        raise ValueError("Unknown marketplace status")
    if date.fromisoformat(row["settlement_date"]) != date.fromisoformat(day) + timedelta(days=15):
        raise ValueError("Invalid scheduled settlement date")
    price, discount, net = [
        decimal_value(row[c]) for c in ("sale_price", "platform_discount", "net_revenue")
    ]
    rate = decimal_value(row["commission_rate"], 10, 6)
    if min(price, discount, net) < 0 or not 0 <= rate <= 1:
        raise ValueError("Invalid price/commission")
    quantity = int(row["quantity_sold"])
    merchandise = price * quantity
    decimal_value(str(merchandise))
    ideal = (merchandise * (1 - rate)).to_integral_value(rounding=ROUND_FLOOR)
    delta = net - ideal
    float_net = int(float(price) * quantity * (1 - float(rate)))
    if delta != 0 and not (delta == -1 and net == float_net):
        raise ValueError("Unexplained marketplace formula mismatch")
    payload = {c: row[c] for c in COLUMNS}
    identity = json.dumps([partner, row["external_order_id"], int(row["shopvn_product_id"])])
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return identity, digest, merchandise, net, delta
