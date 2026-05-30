"""
Delta Lake helpers that encode our idempotency strategy.

The single most important operational property of this pipeline is:
    re-running any day, any number of times, must NOT change the answer
    and must NOT double-count.

Two patterns make that true:

1. `merge_upsert` — for the BRONZE layer, where operators re-send the same
   rows as "corrections". We MERGE on the natural key so a re-sent row
   updates in place instead of appending a duplicate.

2. `overwrite_partition` — for SILVER / GOLD, which are pure functions of the
   layer below. We use Delta's `replaceWhere` to atomically swap out exactly
   the partition(s) we recomputed (e.g. one business_date), leaving every
   other day untouched. Re-running a day = deterministic replace.
"""

from typing import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, path: str) -> bool:
    """True if a Delta table already exists at `path`."""
    return DeltaTable.isDeltaTable(spark, path)


def merge_upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: Sequence[str],
    partition_by: Sequence[str] | None = None,
) -> None:
    """Idempotent upsert (MERGE) into a Delta table on `merge_keys`.

    Used by BRONZE ingestion. If an operator re-sends a transaction, the row
    with the same natural key is overwritten rather than duplicated. New rows
    are inserted. Nothing is ever double-counted downstream because the
    bronze key is unique.
    """
    if not table_exists(spark, path):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return

    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def overwrite_partition(
    df: DataFrame,
    path: str,
    replace_where: str,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Atomically replace only the partitions matching `replace_where`.

    Used by SILVER / GOLD. Example: when we recompute business_date
    2024-01-15, we call this with replace_where="business_date = '2024-01-15'".
    Delta deletes the old 2024-01-15 data and writes the new in one atomic
    commit. Every other date is left exactly as it was — which is precisely
    what lets Finance re-run a day without disturbing a closed month.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_initial(
    df: DataFrame,
    path: str,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Plain overwrite — used for first creation or full rebuilds."""
    writer = df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
