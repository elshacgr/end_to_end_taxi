
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# from taxitransformation import make_unixtime, add_vendor_desc, add_ratecode_desc

default_args = {
    'owner':'elshacgr',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    default_args= default_args,
    dag_id='taxi_airflow_v1',
    description='Airflow green taxi data',
    start_date=datetime(2023, 5, 23),
    schedule='@daily'
) as dag:
    task1 = BashOperator(
        task_id='Scraping_to_local',
        bash_command='python3 /mnt/c/Users/USER/airflow/dags/scrapeTaxi.py',
        dag=dag
    )
    task2 = BashOperator(
        task_id='Save_to_hdfs',
        bash_command='python3 /mnt/c/Users/USER/airflow/dags/copytohdfs.py',
        dag=dag
    )
    task3 = BashOperator(
        task_id='Staging_hive',
        bash_command='python3 /mnt/c/Users/USER/airflow/dags/hivestaging.py',
        dag=dag
    )
    task4 = BashOperator(
        task_id='Transformation',
        bash_command='python3 /mnt/c/Users/USER/airflow/dags/taxitransformation.py',
        dag=dag
    )
    task5 = BashOperator(
        task_id='Create_DimFact_and_savetoHive',
        bash_command='python3 /mnt/c/Users/USER/airflow/dags/dim_fact.py',
        dag=dag
    )

    task1 >> task2 >> task3 >> task4 >> task5

