from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys
import os


sys.path.insert(0, '/opt/airflow/scripts')
from extract_pokemon import extract_pokemon_data

default_args = {
    'owner': 'milagros',
    'start_date': datetime(2025, 10, 5),
    'retries': 1,
}

with DAG('etl_pokemon_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['pokemon', 'dbt']) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_pokemon_data',
        python_callable=extract_pokemon_data
    )
    

    transform_task = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt'
    )
    
    extract_task >> transform_task