from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

from ConvotisPractica.newsapi_extract import extract_news
from ConvotisPractica.load_to_snowflake import load_to_snowflake

with DAG(
    dag_id="news_dag",
    start_date=pendulum.datetime(2025, 4, 1, tz="Europe/Madrid"),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id="extract_news",
        python_callable=extract_news,
        op_args=["{{ data_interval_start }}"], 
    )

    task_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake
    )

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        'cd /opt/airflow/dbtNews && '
        'dbt run'
    )
)

    task_extract >> task_load >> dbt_run
