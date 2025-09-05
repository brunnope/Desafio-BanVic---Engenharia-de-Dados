from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

sys.path.append('/opt/airflow/scripts')
from extract_csv import extract_csv_data
from extract_sql import extract_sql_data
from load_to_dw import load_to_dw

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'banvic_data_pipeline',
    default_args=default_args,
    description='Pipeline ETL BanVic',
    schedule_interval='35 4 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['banvic', 'etl'],
)

def create_directories(**ctx):
    ds = ctx['ds'] 
    
    csv_dir = f"/opt/airflow/extracted_data/{ds}/csv"
    sql_dir = f"/opt/airflow/extracted_data/{ds}/sql"
    
    os.makedirs(csv_dir, exist_ok=True, mode=0o775)
    os.makedirs(sql_dir, exist_ok=True, mode=0o775)
     
    return f"Diretórios criados com sucesso para {ds}"

def extract_csv_task(**ctx):
    return extract_csv_data(ctx['ds'])

def extract_sql_task(**ctx):
    return extract_sql_data(ctx['ds'])

def load_dw_task(**ctx):
    return load_to_dw(ctx['ds'])

prepare_dirs = PythonOperator(
    task_id='prepare_dirs',
    python_callable=create_directories,
    dag=dag,
)
extract_csv = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv_task,
    dag=dag,
)

extract_sql = PythonOperator(
    task_id='extract_sql',
    python_callable=extract_sql_task,
    dag=dag,
)

load_dw = PythonOperator(
    task_id='load_dw',
    python_callable=load_dw_task,
    dag=dag,
)

cleanup = BashOperator(
    task_id='cleanup',
    bash_command='''
    find /opt/airflow/extracted_data -type d -mtime +7 -exec rm -rf {} + || true
    echo "Execução concluída para {{ ds }}"
    ''',
    dag=dag,
)

prepare_dirs >> [extract_csv, extract_sql] >> load_dw >> cleanup
