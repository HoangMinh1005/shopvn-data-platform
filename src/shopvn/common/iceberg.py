"""Iceberg persistence primitives; callers own the Spark session and writer lock."""

from pathlib import Path

from pyspark.sql import functions as F


def with_hash(df, name: str, columns=None):
    fields = sorted(columns or df.columns)
    return df.withColumn(
        name, F.sha2(F.to_json(F.struct(*fields), {"ignoreNullFields": "false"}), 256)
    )


def replace_table(spark, df, table: str, stage: Path):
    # Break self-read lineage before atomic RTAS of an existing Iceberg dimension.
    path = str(stage / table.replace(".", "_"))
    df.coalesce(2).write.mode("overwrite").parquet(path)
    (
        spark.read.parquet(path)
        .writeTo(table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .createOrReplace()
    )


def merge_fact(
    spark,
    df,
    table: str,
    key: str,
    observed_at: str,
    run_id: str,
    *,
    source_system="postgres/shopvn",
    data_origin="source",
):
    business_columns = [c for c in df.columns if c not in {"snapshot_id", "source_record_hash"}]
    df = (
        with_hash(df, "business_hash", business_columns)
        .withColumn("observed_at", F.to_timestamp(F.lit(observed_at)))
        .withColumn("loaded_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_system", F.lit(source_system))
        .withColumn("data_origin", F.lit(data_origin))
    )
    if not spark.catalog.tableExists(table):
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
    else:
        existing = set(spark.table(table).columns)
        for field in df.schema.fields:
            if field.name not in existing:
                spark.sql(
                    f"ALTER TABLE {table} ADD COLUMN `{field.name}` {field.dataType.simpleString()}"
                )
        for field in spark.table(table).schema.fields:
            if field.name not in df.columns:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        df.createOrReplaceTempView("incoming_fact")
        spark.sql(f"""MERGE INTO {table} t USING incoming_fact s ON t.{key}=s.{key}
                  WHEN MATCHED AND t.business_hash<>s.business_hash THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
