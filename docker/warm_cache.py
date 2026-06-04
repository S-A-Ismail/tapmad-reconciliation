"""Build-time helper: start Spark once so the Delta + hadoop-aws (S3A) jars
download into the image layer. That way the container runs without needing
network access at runtime to fetch them.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# hadoop-aws 3.3.4 matches Spark 3.5.1's bundled Hadoop; pulls aws-sdk bundle.
EXTRA_PACKAGES = ["org.apache.hadoop:hadoop-aws:3.3.4"]

builder = SparkSession.builder.master("local[1]").appName("warm-cache")
spark = configure_spark_with_delta_pip(
    builder, extra_packages=EXTRA_PACKAGES
).getOrCreate()
spark.sql("SELECT 1").collect()
spark.stop()
print("delta + hadoop-aws jars cached")
