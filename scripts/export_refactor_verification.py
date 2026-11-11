"""Collect actual refactor regression results; refuse to label incomplete checks passed."""

import hashlib
import json
import os
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    runtime = ROOT / "runtime/refactor"
    suite = ET.parse(runtime / "unit-tests.xml").getroot().find("testsuite")
    unit = {key: int(suite.attrib[key]) for key in ("tests", "failures", "errors", "skipped")}
    if any(unit[key] for key in ("failures", "errors", "skipped")):
        raise RuntimeError("Unit tests are not all passed")
    reports = {}
    for name, path in {
        "spark_layers": runtime / "spark-layer-tests.json",
        "saved_snapshot_replay": runtime / "direct-replay.json",
        "airflow": ROOT / "runtime/airflow/verification.json",
    }.items():
        reports[name] = json.loads(path.read_text())
        if reports[name]["status"] != "passed":
            raise RuntimeError(f"Incomplete or failed evidence: {name}")
    replay = reports["saved_snapshot_replay"]
    if replay["fin02_total_vnd"] != reports["airflow"]["fin02_total_vnd"]:
        raise RuntimeError("Finance amount changed between replay and actual Airflow run")
    wheel = next((runtime / "package").glob("shopvn_data_platform-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        if "shopvn/transformations/direct_sales.py" in archive.namelist():
            raise RuntimeError("Obsolete monolith was included in wheel")
        sql = archive.read("shopvn/ingestion/sql/direct_revenue_baseline.sql")
        config = archive.read("shopvn/config.py")
    interpreter = (
        runtime / "package-check" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    program = """import hashlib,json
from pathlib import Path
from importlib.resources import files
import shopvn.config as c
from shopvn.jobs.direct_finance import main
sql=files('shopvn.ingestion').joinpath('sql/direct_revenue_baseline.sql').read_bytes()
print(json.dumps({'config_sha256':hashlib.sha256(Path(c.__file__).read_bytes()).hexdigest(),'sql_sha256':hashlib.sha256(sql).hexdigest(),'module':c.__file__,'job_import_without_spark':callable(main)}))"""
    installed = json.loads(
        subprocess.check_output([str(interpreter), "-I", "-c", program], cwd=runtime, text=True)
    )
    if (
        installed["config_sha256"] != hashlib.sha256(config).hexdigest()
        or installed["sql_sha256"] != hashlib.sha256(sql).hexdigest()
    ):
        raise RuntimeError("Isolated installation does not match the built wheel")
    if not Path(installed["module"]).is_relative_to(runtime / "package-check"):
        raise RuntimeError("Package test accidentally imported the editable checkout")
    report = {
        "status": "passed",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Refactor only: no table/catalog/volume renaming; current full-snapshot and pre-go-live limits retained",
        "unit_tests": unit,
        "spark_layer_tests": reports["spark_layers"],
        "saved_snapshot_replay": replay,
        "airflow": {
            "status": "passed",
            "blocked": reports["airflow"]["blocked"],
            "recovery": reports["airflow"]["recovery"],
            "fin02_total_vnd": reports["airflow"]["fin02_total_vnd"],
        },
        "wheel": {
            "filename": wheel.name,
            "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "isolated_install": "passed",
            "packaged_sql_sha256": installed["sql_sha256"],
            "job_import_without_spark": installed["job_import_without_spark"],
        },
        "cleanup": "Removed obsolete monolith, duplicate source definitions/runtime imports, placeholder files and legacy-only ERD generator; no runtime/data/secret/volume deletion",
    }
    (ROOT / "docs/verification/refactor.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(
        "Exported passed refactor evidence: unit, real Spark, replay, actual Airflow and isolated wheel"
    )


if __name__ == "__main__":
    main()
