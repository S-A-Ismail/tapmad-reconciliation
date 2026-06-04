# Pipeline + dbt image.
# Runs the PySpark jobs (Spark in local[*] mode) and dbt, all in one container.
# Used by docker-compose for the `pipeline` and `dbt` services.

FROM python:3.11-slim

# Java is required by Spark. default-jdk on bookworm = JDK 17 (supported by Spark 3.5).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# 1. install python deps
COPY docker/requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir -r /tmp/requirements-pipeline.txt

# 1b. bake ALL required jars (Delta + S3A) into pyspark's jars dir, so the
#     container does ZERO Ivy resolution at run time -- no flaky downloads, no
#     corrupt-cache failures. (antlr4-runtime is already in pyspark/jars.)
RUN PYSPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$PYSPARK_JARS/delta-spark_2.12-3.2.0.jar" \
         https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -fsSL -o "$PYSPARK_JARS/delta-storage-3.2.0.jar" \
         https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -fsSL -o "$PYSPARK_JARS/hadoop-aws-3.3.4.jar" \
         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o "$PYSPARK_JARS/aws-java-sdk-bundle-1.12.262.jar" \
         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 2. bake spark-defaults (Delta extension + shared HMS + S3A)
ENV SPARK_CONF_DIR=/opt/sparkconf
COPY docker/spark-defaults.conf /opt/sparkconf/spark-defaults.conf

# 4. project code (bind-mounted over in compose for live edits, but copied so
#    the image is self-contained)
COPY . /app

# all storage paths resolve under here; the volume is shared across services
ENV LAKEHOUSE_ROOT=/lakehouse
ENV PYTHONPATH=/app
RUN mkdir -p /lakehouse

# default: run the whole pipeline end-to-end for one day (override date via
# the BUSINESS_DATE / N env vars)
CMD ["bash", "docker/run_pipeline.sh"]
