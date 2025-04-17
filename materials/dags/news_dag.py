from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow_dbt.operators.dbt import DbtRunOperator
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

    """dbt_run = DbtRunOperator(
        task_id='dbt_run',
        project_dir='C:\\Users\\danis\\Documents\\ImagenesDocker\\dbt\\dbtNews'
    )"""

    task_extract >> task_load #>> dbt_run
