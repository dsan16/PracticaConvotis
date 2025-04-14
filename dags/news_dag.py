from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ConvotisPractica.newsapi_extract import extract_news
from ConvotisPractica.load_to_snowflake import load_to_snowflake

with DAG(
    dag_id="news_dag",
    start_date=datetime(2025, 4, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id="extract_news",
        python_callable=extract_news,
        op_args=["{{ macros.ds_add(ds, -1) }}"], 
    )

    task_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake
    )

    task_extract >> task_load
