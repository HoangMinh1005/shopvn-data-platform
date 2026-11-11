FROM apache/spark:3.5.9@sha256:af02a459c8706e031c835c16f9db3c463816776e2543dd0f828af65606bcf392 AS distribution
# Generic apache/spark:3.5.9 has Python 3.10 / Java 11. Align our runtime explicitly.
FROM python:3.11.15-slim-bookworm
RUN apt-get -o Acquire::Retries=3 update && apt-get -o Acquire::Retries=3 install -y --no-install-recommends openjdk-17-jre-headless procps
COPY --from=distribution /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark PYSPARK_PYTHON=/usr/local/bin/python PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
COPY runtime/spike-build/*.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar
WORKDIR /workspace
