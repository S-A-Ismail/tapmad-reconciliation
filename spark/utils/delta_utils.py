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

    `replaceWhere` requires an existing, compatibly-partitioned table and CANNOT
    be combined with `overwriteSchema`. So we only use it for steady-state,
    in-place replaces; the first write (or a corrupt/incompatibly-partitioned
    table left by an aborted run) is handled with a clean full overwrite.
    """
    spark = df.sparkSession

    def _full_overwrite() -> None:
        # Force STATIC partition overwrite for the full create/reset: the
        # session runs in dynamic mode (for dbt's insert_overwrite), and Delta
        # forbids overwriteSchema together with dynamic partition overwrite.
        writer = (
            df.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .option("partitionOverwriteMode", "static")
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

    # First ever write: establish the schema + partitioning.
    if not table_exists(spark, path):
        _full_overwrite()
        return

    # Existing table whose partitioning doesn't match (e.g. an empty/corrupt
    # table from an earlier failed run) -> reset it; replaceWhere can't.
    current_parts = (
        DeltaTable.forPath(spark, path).detail().select("partitionColumns").first()[0]
    )
    if list(current_parts or []) != list(partition_by or []):
        _full_overwrite()
        return

    # Steady state: atomic per-partition replace.
    writer = (
        df.write.format("delta").mode("overwrite").option("replaceWhere", replace_where)
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
