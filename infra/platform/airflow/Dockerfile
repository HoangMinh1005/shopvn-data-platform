FROM shopvn-readiness-spark:3.5.9-iceberg1.12.0
ADD https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt /opt/airflow-constraints.txt
RUN python -m venv /opt/airflow-venv && /opt/airflow-venv/bin/pip install --no-cache-dir \
    'apache-airflow[postgres]==3.4.0' 'apache-airflow-providers-apache-spark==6.3.4' \
    'psycopg[binary]' prometheus-client --constraint /opt/airflow-constraints.txt
ENV PATH=/opt/airflow-venv/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin \
    AIRFLOW_HOME=/opt/airflow PYTHONPATH=/workspace/src:/workspace/scripts:/opt/airflow/plugins
WORKDIR /workspace
