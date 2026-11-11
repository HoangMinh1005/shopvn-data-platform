"""Export successful aggregate evidence and exact image/JAR identities, excluding source rows."""

import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGES = {
    "spark": "shopvn-readiness-spark:3.5.9-iceberg1.12.0",
    "minio": "shopvn-readiness-minio:9e49d5e7",
    "trino": "trinodb/trino:484",
    "catalog": "postgres:16@sha256:2586e2a95d1c9b31cb2967feb562948f7d364854453d703039b6efa45fe48417",
    "python_base": "python:3.11.15-slim-bookworm",
}


def main():
    destination = ROOT / "docs/verification"
    destination.mkdir(parents=True, exist_ok=True)
    for source, filename in (("runtime/spike/spike-report.json", "storage-spike.json"),
                             ("runtime/direct/direct-slice-report.json", "direct-finance.json")):
        report = json.loads((ROOT / source).read_text())
        if report["status"] != "passed":
            raise RuntimeError(f"Do not export failed evidence as successful: {source}")
        (destination / filename).write_text(json.dumps(report, indent=2), encoding="utf-8")
    images = {}
    for name, image in IMAGES.items():
        metadata = json.loads(subprocess.check_output(["docker", "image", "inspect", image], text=True))[0]
        images[name] = {"reference": image, "image_id": metadata["Id"],
                        "repo_digests": metadata.get("RepoDigests", []),
                        "architecture": metadata["Architecture"], "os": metadata["Os"]}
    jars = {}
    for path in (ROOT / "runtime/spike-build").glob("*.jar"):
        with path.open("rb") as stream:
            jars[path.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    storage = json.loads((destination / "storage-spike.json").read_text())
    if jars != storage["runs"][-1]["jar_sha256"]:
        raise ValueError("Local JAR artifacts differ from the files observed inside tested Spark")
    source = ROOT / "runtime/spike-build/minio-source.tar.gz"
    with source.open("rb") as stream:
        source_sha = hashlib.file_digest(stream, "sha256").hexdigest()
    lock = {"scope": "Verified core only; Airflow/Kafka/Superset runtime not yet validated",
            "images": images, "jar_sha256": jars,
            "minio_source_commit": "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a",
            "minio_source_archive_sha256": source_sha,
            "spark_runtime": {"spark": "3.5.9", "scala": "2.12.18", "python": "3.11.15", "java": "17.0.21",
                              "debian_openjdk_package": "17.0.21+9-1~deb12u1"}}
    (ROOT / "infra/platform/spike/verified-lock.json").write_text(json.dumps(lock, indent=2), encoding="utf-8")
    print("Exported passed storage/direct evidence and verified core lock")


if __name__ == "__main__":
    main()
