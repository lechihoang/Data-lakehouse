FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl tar git && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow providers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# dbt in separate venv (isolated to avoid Airflow dep conflicts)
USER root
RUN python -m venv /opt/dbt_venv && \
    chown -R airflow:0 /opt/dbt_venv
USER airflow
RUN /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.5 dbt-trino==1.8.5
