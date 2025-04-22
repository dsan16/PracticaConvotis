FROM apache/airflow:2.6.3-python3.9
USER airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.6.0"                         \
    "dbt-snowflake==1.6.0"
USER root
