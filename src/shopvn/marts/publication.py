"""Publish only the exact candidate snapshot approved by the preceding Airflow DQ task."""

import json

from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table
from shopvn.common.spark_runtime import acquire_writer_lock, session
from shopvn.orchestration.artifacts import folder, load


def publish(key: str):
    root = folder(key)
    approved = load(key, "validation.json")
    if not approved["passed"]:
        raise ValueError("Publish requires a passed reconciliation artifact")
    spark = session("shopvn-publish-fin02")
    lock = None
    try:
        lock = acquire_writer_lock(spark)
        candidate = f"shopvn.staging.mart_fin02_{key}"
        snapshot = int(approved["candidate_snapshot_id"])
        current = (
            spark.table(candidate + ".snapshots")
            .orderBy(F.col("committed_at").desc())
            .first()["snapshot_id"]
        )
        if current != snapshot:
            raise ValueError("Candidate changed after approval; rerun validation")
        data = spark.read.format("iceberg").option("snapshot-id", str(snapshot)).load(candidate)
        target = "shopvn.analytics.mart_fin02_daily"
        already = False
        if spark.catalog.tableExists(target):
            previous = spark.table(target)
            if "source_candidate_snapshot_id" in previous.columns:
                publication = (
                    previous.select("publication_id", "source_candidate_snapshot_id")
                    .distinct()
                    .collect()
                )
                already = (
                    len(publication) == 1
                    and publication[0][0] == key
                    and publication[0][1] == snapshot
                )
            if (
                "data_as_of" in previous.columns
                and previous.agg(F.max("data_as_of")).first()[0]
                > data.agg(F.max("data_as_of")).first()[0]
            ):
                raise ValueError("Refuse to overwrite a newer publication with an older snapshot")
        if not already:
            replace_table(
                spark,
                data.withColumn("source_candidate_snapshot_id", F.lit(snapshot).cast("long")),
                target,
                root / "publish_stage",
            )
        (root / "publication.json").write_text(
            json.dumps(
                {
                    "status": "published",
                    "candidate_snapshot_id": snapshot,
                    "idempotent_skip": already,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print("Published the validated candidate snapshot", flush=True)
    finally:
        if lock is not None:
            lock.close()
        spark.stop()
