# AdPulse Spark image: official Apache Spark 3.5.7 (Java 17, Python 3) + connector JARs.
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

# Optional extra CA certificates (only needed behind a TLS-inspecting proxy;
# on a normal home computer docker/certs/ is empty and this does nothing).
COPY docker/certs/ /tmp/extra-certs/
RUN if ls /tmp/extra-certs/*.crt >/dev/null 2>&1; then \
      cp /tmp/extra-certs/*.crt /usr/local/share/ca-certificates/ && update-ca-certificates; \
    fi

# Connector JARs, versions matched to Spark 3.5.7 / Hadoop 3.3.4:
#  - Kafka source for Structured Streaming (+ its dependencies)
#  - hadoop-aws + AWS SDK bundle for s3a:// (S3, MinIO, SeaweedFS ...)
#  - PostgreSQL JDBC driver
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar ; do \
        curl -fsSL --retry 5 --retry-delay 3 -O "$MAVEN/$jar" || exit 1; \
    done && ls -la /opt/spark/jars | grep -E "kafka|aws|postgres|pool2"

COPY requirements/spark.txt /tmp/requirements-spark.txt
RUN PIP_CERT=/etc/ssl/certs/ca-certificates.crt pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

WORKDIR /opt/adpulse
COPY pyproject.toml ./
COPY spark_jobs/ ./spark_jobs/
COPY tests/__init__.py ./tests/__init__.py
COPY tests/spark/ ./tests/spark/
RUN mkdir -p /opt/adpulse/checkpoints && chown -R spark:spark /opt/adpulse

ENV PYTHONPATH=/opt/adpulse/spark_jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3
USER spark
