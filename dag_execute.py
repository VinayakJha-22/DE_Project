from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'etl_batch_dag',
    default_args=default_args,
    description='DAG to execute ETL batch job daily',
    schedule_interval='0 0 * * *',  # Execute at midnight every day
    start_date=days_ago(1),
    tags=['etl', 'batch']
)

# Define the commands to execute
execute_command = 'python /home/airflow/gcs/data/"ETL Scripts"/main.py'

# Define the tasks
execute_task = BashOperator(
    task_id='execute_etl_batch',
    bash_command=execute_command,
    dag=dag
)

# Set task dependencies
execute_task
