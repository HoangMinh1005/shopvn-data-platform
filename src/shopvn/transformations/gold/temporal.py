"""Observed SCD2 and explicitly day-grain dimension lookup for date-only orders."""

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from shopvn.common.iceberg import replace_table, with_hash
from shopvn.common.time import utc_instant
from shopvn.quality.spark import fail_if
from shopvn.quality.temporal import assert_observation_order, validate_history


def observed_dimension(spark, incoming, table, key, business, observed_at, stage):
    observed_at = utc_instant(observed_at).isoformat()
    assert_observation_order(spark, observed_at, (table,))
    fail_if(incoming, F.col(business).isNull(), "Null dimension business key")
    if incoming.select(business).distinct().count() != incoming.count():
        raise ValueError("Duplicate dimension business key")
    incoming = with_hash(incoming, "attribute_hash")
    prior = spark.table(table) if spark.catalog.tableExists(table) else None
    instant = F.to_timestamp(F.lit(observed_at))
    if prior is not None:
        validate_history(prior, business, key)
        if "last_observed_at" not in prior.columns:
            prior = prior.withColumn("last_observed_at", F.col("valid_from"))
        current = prior.filter(F.col("is_current") & (F.col(key) != 0)).select(
            business,
            F.col("attribute_hash").alias("previous_hash"),
            F.col("last_observed_at").alias("previous_observed_at"),
        )
        compared = incoming.join(current, business, "left")
        changed = compared.filter(
            F.col("previous_hash").isNull() | (F.col("attribute_hash") != F.col("previous_hash"))
        )
        if (
            prior.filter(F.col("last_observed_at") == instant).limit(1).count()
            and changed.limit(1).count()
        ):
            raise ValueError("Conflicting payload at the same observation timestamp")
        changed = changed.drop("previous_hash", "previous_observed_at")
        if changed.limit(1).count() == 0:
            seen = incoming.select(business).withColumn("seen", F.lit(True))
            result = (
                prior.join(seen, business, "left")
                .withColumn(
                    "last_observed_at",
                    F.when(F.col("seen") & F.col("is_current"), instant).otherwise(
                        F.col("last_observed_at")
                    ),
                )
                .drop("seen")
            )
            replace_table(spark, result, table, stage)
            return spark.table(table)
        maximum = prior.agg(F.max(key)).first()[0] or 0
    else:
        changed, maximum = incoming, 0
    new = (
        changed.withColumn(
            key, (F.row_number().over(Window.orderBy(business)) + maximum).cast("long")
        )
        .withColumn("valid_from", instant)
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("history_reliable", F.lit(False))
        .withColumn("last_observed_at", instant)
    )
    if prior is None:
        defaults = {
            key: 0,
            business: "0" if business == "customer_ref" else 0,
            "is_current": True,
            "history_reliable": False,
            "attribute_hash": "unknown",
            "valid_from": datetime(1970, 1, 1, tzinfo=timezone.utc),
        }
        unknown = spark.range(1).select(
            *[
                F.lit(defaults.get(field.name)).cast(field.dataType).alias(field.name)
                for field in new.schema.fields
            ]
        )
        result = new.unionByName(unknown)
    else:
        marker = changed.select(business).withColumn("changed", F.lit(True))
        seen = incoming.select(business).withColumn("seen", F.lit(True))
        history = (
            prior.join(marker, business, "left")
            .join(seen, business, "left")
            .withColumn(
                "valid_to",
                F.when(F.col("changed") & F.col("is_current"), instant).otherwise(
                    F.col("valid_to")
                ),
            )
            .withColumn(
                "is_current",
                F.when(F.col("changed") & F.col("is_current"), F.lit(False)).otherwise(
                    F.col("is_current")
                ),
            )
            .withColumn(
                "last_observed_at",
                F.when(F.col("seen") & F.col("is_current"), instant).otherwise(
                    F.col("last_observed_at")
                ),
            )
            .drop("changed", "seen")
        )
        result = history.unionByName(new)
    validate_history(result, business, key)
    replace_table(spark, result, table, stage)
    return spark.table(table)


def lookup_day_versions(requests, versions, business, key, observed_at):
    """Latest version observed by min(day-end VN, extraction time); not exact purchase-time history."""
    validate_history(versions, business, key)
    requests = requests.select(business, "order_date").distinct()
    fail_if(
        requests,
        F.col(business).isNull() | F.col("order_date").isNull(),
        "Null as-of lookup request",
    )
    first = (
        versions.filter(F.col(key) != 0)
        .groupBy(business)
        .agg(F.min("valid_from").alias("first_observed_at"))
    )
    requested = (
        requests.join(first, business, "left")
        .withColumn(
            "initial_fallback",
            F.col("order_date")
            < F.to_date(F.from_utc_timestamp("first_observed_at", "Asia/Ho_Chi_Minh")),
        )
        .withColumn(
            "lookup_at",
            F.least(
                F.to_timestamp(F.lit(utc_instant(observed_at).isoformat())),
                F.to_utc_timestamp(
                    F.date_add("order_date", 1).cast("timestamp"), "Asia/Ho_Chi_Minh"
                )
                - F.expr("INTERVAL 1 MICROSECOND"),
            ),
        )
    )
    requested = requested.withColumn(
        "initial_fallback",
        F.col("initial_fallback") | (F.col("lookup_at") < F.col("first_observed_at")),
    )
    r, d = requested.alias("r"), versions.filter(F.col(key) != 0).alias("d")
    condition = (F.col("r." + business) == F.col("d." + business)) & (
        (F.col("r.initial_fallback") & (F.col("d.valid_from") == F.col("r.first_observed_at")))
        | (
            ~F.col("r.initial_fallback")
            & (F.col("d.valid_from") <= F.col("r.lookup_at"))
            & (F.col("d.valid_to").isNull() | (F.col("r.lookup_at") < F.col("d.valid_to")))
        )
    )
    result = r.join(d, condition, "left").select(
        F.col("r." + business).alias(business),
        F.col("r.order_date").alias("order_date"),
        F.col("d." + key).alias(key),
        F.when(F.col("r.initial_fallback"), "initial_snapshot_fallback")
        .otherwise("observed_day_asof")
        .alias("dimension_match_basis"),
    )
    fail_if(result, F.col(key).isNull(), "No observed dimension version for requested day")
    if result.count() != requests.count():
        raise ValueError("Ambiguous as-of lookup")
    return result
