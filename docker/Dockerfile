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

# 2. warm the Delta jar cache (no SPARK_CONF_DIR yet, so no metastore needed)
COPY docker/warm_cache.py /tmp/warm_cache.py
RUN python /tmp/warm_cache.py

# 3. bake spark-defaults (Delta + local Hive metastore on the shared volume)
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
