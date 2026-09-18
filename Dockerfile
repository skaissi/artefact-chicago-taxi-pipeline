# Pin Airflow + Python for repeatable local builds.
FROM apache/airflow:3.2.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /opt/airflow/project
COPY --chown=airflow:root requirements.txt ./requirements.txt
# Pin Airflow itself to avoid an incidental upgrade when resolving dependencies.
RUN pip install --no-cache-dir "apache-airflow==3.2.2" -r requirements.txt

# Spark 3.5.5 bundles Hadoop 3.3.4: install compatible MinIO/S3A jars.
RUN SPARK_JARS="$(python -c 'import pathlib,pyspark;print(pathlib.Path(pyspark.__file__).parent / "jars")')" && \
    curl --fail --location --retry 4 --silent --show-error \
      https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      --output "${SPARK_JARS}/hadoop-aws-3.3.4.jar" && \
    curl --fail --location --retry 4 --silent --show-error \
      https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      --output "${SPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar"

COPY --chown=airflow:root . /opt/airflow/project/
ENV PYTHONPATH=/opt/airflow/project/src \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
