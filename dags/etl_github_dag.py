from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Agregar directorio raíz para importar ETLPipeline
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from bot_local import ETLPipeline

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

pipeline = ETLPipeline(
    api_url="https://api.github.com/events",
    raw_dir="./raw_data",
    processed_dir="./processed_data",
    db_path="etl_data.db"
)

def run_extract(**kwargs):
    raw_file_path, raw_data = pipeline.extract()
    kwargs['ti'].xcom_push(key='raw_data', value=raw_data)

def run_transform(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_task', key='raw_data')
    processed_df = pipeline.transform(raw_data)
    kwargs['ti'].xcom_push(key='processed_data', value=processed_df.to_dict())

def run_load(**kwargs):
    import pandas as pd
    data_dict = kwargs['ti'].xcom_pull(task_ids='transform_task', key='processed_data')
    processed_df = pd.DataFrame.from_dict(data_dict)
    pipeline.load(processed_df)
    pipeline.load_to_bigquery()

with DAG(
    'github_events_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL automatizado de eventos GitHub para GCP Cloud Composer',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=run_load,
    )

    # Dependencias de las tareas (Workflow)
    extract_task >> transform_task >> load_task