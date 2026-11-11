"""Operational audit in a separate platform database; no source writes or notification sends."""

import hashlib
import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
 run_key text PRIMARY KEY, dag_id text NOT NULL, dag_run_id text NOT NULL,
 run_type text NOT NULL, business_date date NOT NULL, status text NOT NULL,
 stage text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 snapshot_id text, candidate_snapshot_id bigint, source_amount numeric(30,2),
 candidate_amount numeric(30,2), amount_delta numeric(30,2), dq_passed boolean,
 source_min_date date, source_max_date date, published boolean NOT NULL DEFAULT false,
 error_class text, attempts integer NOT NULL DEFAULT 0, rule_version text NOT NULL DEFAULT '1.1'
);
CREATE TABLE IF NOT EXISTS ops.alert (
 alert_key text PRIMARY KEY, run_key text, business_date date, kind text NOT NULL,
 severity text NOT NULL, message text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 resolved_at timestamptz, resolved_by_run_key text
);
"""


def run_key(run_id: str) -> str:
    return hashlib.sha256(("shopvn_direct_finance:" + run_id).encode()).hexdigest()[:24]


@contextmanager
def connection():
    with psycopg.connect(
        host=os.getenv("OPS_DB_HOST", "catalog"),
        dbname=os.getenv("OPS_DB_NAME", "airflow_meta"),
        user=os.getenv("OPS_DB_USER", "airflow"),
        password=os.environ["OPS_DB_PASSWORD"],
        connect_timeout=5,
        row_factory=dict_row,
    ) as conn:
        yield conn


def initialize_schema():
    with connection() as conn:
        conn.execute(DDL)


def record_start(key, dag_id, dag_run_id, run_type, business_date):
    with connection() as conn:
        conn.execute(
            """INSERT INTO ops.pipeline_run(run_key,dag_id,dag_run_id,run_type,business_date,status,stage)
                     VALUES(%s,%s,%s,%s,%s,'running','initialize')
                     ON CONFLICT(run_key) DO UPDATE SET status='running',finished_at=NULL,stage='initialize'""",
            (key, dag_id, dag_run_id, run_type, business_date),
        )


def update_run(key, **values):
    allowed = {
        "status",
        "stage",
        "snapshot_id",
        "candidate_snapshot_id",
        "source_amount",
        "candidate_amount",
        "amount_delta",
        "dq_passed",
        "source_min_date",
        "source_max_date",
        "published",
        "error_class",
        "attempts",
    }
    if not set(values) <= allowed:
        raise ValueError("Unknown ops audit field")
    assignments = ",".join(f"{name}=%s" for name in values)
    if values.get("status") in {"success", "failed"}:
        assignments += ",finished_at=now()"
    with connection() as conn:
        conn.execute(
            f"UPDATE ops.pipeline_run SET {assignments} WHERE run_key=%s", (*values.values(), key)
        )


def alert(key, business_date, kind, message, severity="critical", suffix=""):
    identity = hashlib.sha256(f"{key}:{kind}:{suffix}".encode()).hexdigest()
    with connection() as conn:
        conn.execute(
            """INSERT INTO ops.alert(alert_key,run_key,business_date,kind,severity,message)
                      VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(alert_key) DO NOTHING""",
            (identity, key, business_date, kind, severity, message),
        )


def finish_success(key):
    with connection() as conn:
        conn.execute(
            """UPDATE ops.pipeline_run SET status='success',stage='complete',published=true,
                     finished_at=now(),error_class=NULL WHERE run_key=%s""",
            (key,),
        )
        conn.execute(
            """UPDATE ops.alert SET resolved_at=now(),resolved_by_run_key=%s
                     WHERE resolved_at IS NULL AND business_date=(SELECT business_date FROM ops.pipeline_run WHERE run_key=%s)
                     AND kind IN ('pipeline_failed','validation_failed','task_retry','deadline_runtime')""",
            (key, key),
        )
