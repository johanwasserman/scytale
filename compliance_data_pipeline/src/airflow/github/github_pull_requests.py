from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(year=2024, month=1, day=1, hour=1),  # Start DAG execution at 1 AM on Jan 1, 2024
    'schedule_interval': '@daily',  # Run daily @ 1AM
}

# Create a new DAG
dag = DAG(
    'github_pr_etl_dag',
    default_args=default_args,
    description='Extract and transform GitHub data',
    catchup=False  # Skip any historical DAG runs
)

# Define PythonOperator for pr_extract.py
extract_task = PythonOperator(
    task_id='extract_github_data',
    python_callable=lambda: subprocess.run(["python", "/path/to/extract/github/pr_extract.py"]),
    dag=dag
)

# Define PythonOperator for pr_transform.py
transform_task = PythonOperator(
    task_id='transform_github_data',
    python_callable=lambda: subprocess.run(["python", "/path/to/transform/github/pr_transform.py"]),
    dag=dag
)

# Set task dependencies
extract_task >> transform_task
