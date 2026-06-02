"""Build-time helper: start Spark once so the Delta/Ivy jars download into the
image layer. That way the container runs without needing network access at
runtime to fetch io.delta:delta-spark.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder.master("local[1]").appName("warm-cache")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sql("SELECT 1").collect()
spark.stop()
print("delta jars cached")
