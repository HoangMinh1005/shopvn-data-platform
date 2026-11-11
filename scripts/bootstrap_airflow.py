"""Initialize local Airflow secrets and a separate metadata database, never the source DB."""

import base64
import json
import secrets
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]


def main():
    env_file = ROOT / ".env"
    existing = {}
    for line in env_file.read_text().splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            existing[key.strip()] = value.strip()
    generated = {
        "AIRFLOW_DB_PASSWORD": secrets.token_urlsafe(32),
        "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(48),
        "AIRFLOW_API_SECRET": secrets.token_urlsafe(48),
        "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "MONITOR_START_DATE": (datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")) + timedelta(days=1))
        .date()
        .isoformat(),
    }
    missing = {
        key: value
        for key, value in generated.items()
        if not existing.get(key) or existing[key].startswith("replace-with-")
    }
    with env_file.open("a", encoding="utf-8") as stream:
        if missing:
            stream.write("\n# Local Airflow/observability configuration; never commit .env.\n")
        for key, value in missing.items():
            stream.write(f"{key}={value}\n")
    existing.update(missing)
    runtime = ROOT / "runtime/airflow"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "simple_auth_passwords.json").write_text(
        json.dumps({"admin": existing["AIRFLOW_ADMIN_PASSWORD"]}), encoding="utf-8"
    )
    (runtime / "local-access.json").write_text(
        json.dumps(
            {
                "airflow_url": "http://localhost:18081",
                "grafana_url": "http://localhost:13000",
                "username": "admin",
                "airflow_password": existing["AIRFLOW_ADMIN_PASSWORD"],
                "grafana_password": existing["GRAFANA_ADMIN_PASSWORD"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    password = sql.Literal(existing["AIRFLOW_DB_PASSWORD"]).as_string()
    statement = f"""DO $body$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='airflow') THEN
        CREATE ROLE airflow LOGIN PASSWORD {password};
      END IF;
    END $body$;
    SELECT 'CREATE DATABASE airflow_meta OWNER airflow'
    WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='airflow_meta')\gexec
    """
    command = [
        "docker",
        "compose",
        "--project-name",
        "shopvn-readiness",
        "--env-file",
        str(env_file),
        "-f",
        str(ROOT / "infra/platform/spike/compose.yml"),
        "exec",
        "-T",
        "catalog",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "iceberg",
        "-d",
        "iceberg_catalog",
    ]
    subprocess.run(command, input=statement, text=True, check=True, capture_output=True)
    print("Initialized separate Airflow metadata DB and local credentials (not printed).")
    print("Local login details: runtime/airflow/local-access.json")


if __name__ == "__main__":
    main()
