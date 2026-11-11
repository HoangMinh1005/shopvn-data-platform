"""Read and verify landing snapshots, append each snapshot to Bronze at most once."""

import hashlib
from pathlib import Path

from pyspark.sql import functions as F

from shopvn.common.iceberg import with_hash
from shopvn.contracts.direct import PRIMARY_KEYS
from shopvn.quality.spark import fail_if


def load_table(spark, name: str, schema: str, manifest: dict, snapshot_id: str, landing: Path):
    path = landing / snapshot_id / f"{name}.csv"
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["tables"][name]["sha256"]:
            raise ValueError(f"Source snapshot checksum failed: {name}")
    frame = (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "FAILFAST")
        .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss[.SSSSSS]")
        .csv(str(path))
        .cache()
    )
    key = PRIMARY_KEYS[name]
    count = frame.count()
    if count != manifest["tables"][name]["rows"] or frame.select(key).distinct().count() != count:
        raise ValueError(f"Count/identity reconciliation failed: {name}")
    fail_if(frame, F.col(key).isNull(), f"Null source identity: {name}")
    frame = with_hash(frame, "source_record_hash").withColumn("snapshot_id", F.lit(snapshot_id))
    bronze = f"shopvn.bronze.direct_{name}"
    if not spark.catalog.tableExists(bronze):
        frame.writeTo(bronze).using("iceberg").tableProperty("format-version", "2").create()
    elif spark.table(bronze).filter(F.col("snapshot_id") == snapshot_id).count() == 0:
        frame.writeTo(bronze).append()
    persisted_count = spark.table(bronze).filter(F.col("snapshot_id") == snapshot_id).count()
    if persisted_count != count:
        raise ValueError(f"Bronze replay count mismatch: {name}")

    return frame
