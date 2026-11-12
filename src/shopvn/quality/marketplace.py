"""Independent source reference → exact pinned Trino candidate → approval artifact."""

import hashlib
import json
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

from shopvn.clients.trino import trino_query
from shopvn.config import ProjectPaths


def run_folder(key):
    import re

    if not re.fullmatch(r"[a-f0-9]{24}", key):
        raise ValueError("Invalid marketplace run key")
    return ProjectPaths.from_env().runtime / "marketplace/runs" / key


def validate(key, *, force_failure=False):
    root = run_folder(key)
    candidate = json.loads((root / "candidate.json").read_text())
    report = {
        "passed": False,
        "candidate_sha256": hashlib.sha256(
            json.dumps(candidate, sort_keys=True).encode()
        ).hexdigest(),
    }
    try:
        if candidate["status"] != "passed":
            raise ValueError("Candidate transform did not pass")
        item = candidate["tables"]["mart"]
        table = item["name"].replace("shopvn.", "lakehouse.", 1)
        rows = trino_query(
            f"SELECT CAST(business_date AS VARCHAR),channel,CAST(comparable_merchandise AS VARCHAR),CAST(partner_reported_net AS VARCHAR),eligible_orders,units_sold,CAST(aov_merchandise AS VARCHAR) FROM {table} FOR VERSION AS OF {int(item['snapshot_id'])} WHERE report_type='sales' AND channel NOT LIKE 'direct_%'"
        )
        actual = {(r[1], r[0]): r for r in rows}
        expected_keys = {(r["partner"], r["business_date"]) for r in candidate["expected"]}
        for identity, row in actual.items():
            if identity not in expected_keys and any(
                row[index] is not None for index in (2, 3, 4, 5, 6)
            ):
                raise ValueError("Missing source scope was converted into amounts/counts")
        source_total = sum(Decimal(r["reported_net"]) for r in candidate["expected"])
        target_total = sum(Decimal(r[3]) for r in rows if r[3] is not None)
        report.update(
            source_reported_net=str(source_total),
            reported_net_total=str(target_total),
            reconciliation_delta=str(target_total - source_total),
        )
        for expected in candidate["expected"]:
            row = actual[(expected["partner"], expected["business_date"])]
            values = (Decimal(row[2]), Decimal(row[3]), row[4], row[5])
            wanted = (
                Decimal(expected["merchandise"]),
                Decimal(expected["reported_net"]),
                expected["eligible_orders"],
                expected["units"],
            )
            if values != wanted:
                raise ValueError("Trino marketplace candidate/source reference mismatch")
            aov = (
                (wanted[0] / wanted[2]).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                if wanted[2]
                else None
            )
            if (Decimal(row[6]) if row[6] is not None else None) != aov:
                raise ValueError("Invalid marketplace AOV")
        settlement = trino_query(
            f"SELECT CAST(report_date AS VARCHAR),channel,CAST(partner_reported_net AS VARCHAR),CAST(comparable_merchandise AS VARCHAR) FROM {table} FOR VERSION AS OF {int(item['snapshot_id'])} WHERE report_type='scheduled_settlement'"
        )
        settle_map = {(r[1], r[0]): r for r in settlement}
        for expected in candidate["expected"]:
            day = (date.fromisoformat(expected["business_date"]) + timedelta(days=15)).isoformat()
            row = settle_map[(expected["partner"], day)]
            if Decimal(row[2]) != Decimal(expected["reported_net"]) or row[3] is not None:
                raise ValueError("Scheduled settlement/source reference mismatch")
        item_fact = candidate["tables"]["facts"]
        fact_table = item_fact["name"].replace("shopvn.", "lakehouse.", 1)
        fact_rows = trino_query(f"""SELECT substr(source_system,13),order_date_key,count(*),
            CAST(sum(CASE WHEN is_sales_eligible THEN net_merchandise_before_refund ELSE 0 END) AS VARCHAR),
            CAST(sum(CASE WHEN is_sales_eligible THEN partner_reported_net_amount ELSE 0 END) AS VARCHAR),
            sum(CASE WHEN is_sales_eligible THEN quantity ELSE 0 END)
            FROM {fact_table} FOR VERSION AS OF {int(item_fact["snapshot_id"])}
            WHERE source_system LIKE 'marketplace/%' AND data_origin='source' GROUP BY 1,2""")
        fact_map = {(r[0], str(r[1])): r for r in fact_rows}
        for expected in candidate["expected"]:
            row = fact_map.get((expected["partner"], expected["business_date"].replace("-", "")))
            values = (
                (row[2], Decimal(row[3]), Decimal(row[4]), row[5])
                if row
                else (0, Decimal(0), Decimal(0), 0)
            )
            wanted = (
                expected["accepted"],
                Decimal(expected["merchandise"]),
                Decimal(expected["reported_net"]),
                expected["units"],
            )
            if values != wanted:
                raise ValueError("Conformed marketplace fact/source reference mismatch")
        from shopvn.ingestion.postgres import finance_reference

        expected_direct = {
            (str(r["order_date"]), "direct_" + r["channel"]): (
                r["eligible_order_count"],
                r["merchandise_before_refund"],
                r["net_customer_amount"],
            )
            for r in finance_reference()
        }
        direct_rows = trino_query(
            f"SELECT CAST(business_date AS VARCHAR),channel,eligible_orders,CAST(comparable_merchandise AS VARCHAR),CAST(direct_net_customer_amount AS VARCHAR) FROM {table} FOR VERSION AS OF {int(item['snapshot_id'])} WHERE report_type='sales' AND channel LIKE 'direct_%'"
        )
        actual_direct = {(r[0], r[1]): (r[2], Decimal(r[3]), Decimal(r[4])) for r in direct_rows}
        if actual_direct != expected_direct:
            raise ValueError(
                "Independent PostgreSQL FIN01/FIN02/count reference differs; refresh direct Finance first"
            )
        report["direct_reference_scopes"] = len(expected_direct)
        if force_failure:
            raise ValueError("Injected marketplace DQ failure; publication blocked")
        report.update(
            passed=True,
            checked_scopes=len(candidate["expected"]),
            checked_rows=len(rows),
            reported_net_total=str(sum(Decimal(r[3]) for r in rows if r[3] is not None)),
        )
        return report
    finally:
        (root / "validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
