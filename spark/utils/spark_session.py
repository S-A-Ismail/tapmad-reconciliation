"""
Spark session factory.

Locally we run open-source Spark + delta-spark so the whole pipeline can be
executed on a laptop with no cloud account. On Databricks, the `spark`
session already exists in the notebook, so `get_spark()` simply returns the
active session. This keeps the job code identical in both environments.
"""

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "tapmad-reconciliation",
    use_hive: bool = False,
) -> SparkSession:
    """Return an active SparkSession with Delta Lake enabled.

    On Databricks: returns the pre-existing session (the builder call is a
    no-op that picks up the running cluster).
    Locally: spins up a session configured with the Delta extension.

    `use_hive` controls the SQL catalog:
      * False (default) -> in-memory catalog. The bronze/silver/gold jobs only
        read/write Delta BY PATH, so they need no catalog. Crucially this also
        avoids booting the embedded-Derby Hive metastore, which only allows a
        single JVM at a time -- when Airflow runs these jobs in parallel, a
        shared Derby metastore would make the second job fail to start.
      * True -> use the Hive metastore from spark-defaults.conf. Needed only by
        the jobs that register / read catalog tables (register_tables,
        export_marts_parquet). Those run as solo, sequential tasks, so the
        single-JVM Derby limitation is not a problem for them.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake wiring. On Databricks these are already set; setting
        # them again is harmless.
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Sensible local defaults. On a real cluster these are tuned per job.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    if not use_hive:
        # Use an in-memory catalog so path-only jobs (bronze/silver/gold) don't
        # connect to the Hive metastore at all -- they read/write Delta by path.
        builder = builder.config("spark.sql.catalogImplementation", "in-memory")

    # NOTE: all jars (delta-spark, delta-storage, hadoop-aws, aws-sdk) are baked
    # into pyspark's jars dir at image build, so we do NOT call
    # configure_spark_with_delta_pip / set spark.jars.packages -- that would
    # trigger Ivy resolution at run time, which has proven flaky/cache-corrupting.
    # The Delta SQL extension + catalog are configured above and in spark-defaults.
    return builder.getOrCreate()
