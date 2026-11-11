"""Exact decimal reconciliation shared by Spark output and Trino validation."""

from decimal import Decimal


def expected_fin02(manifest):
    return {
        (row["order_date"], row["channel"]): (
            row["eligible_order_count"],
            Decimal(row["net_customer_amount"]),
        )
        for row in manifest["fin02_expected_by_day_channel"]
    }


def queried_fin02(rows):
    return {(row[0], row[1]): (row[2], Decimal(row[3])) for row in rows}


def amount_for_day(values, day):
    return sum(
        (amount for (row_day, _), (_, amount) in values.items() if row_day == day), Decimal("0")
    )


def assert_fin02_matches(actual, expected):
    if actual != expected:
        raise ValueError("FIN02 source-to-Gold reconciliation failed; certified mart not replaced")
