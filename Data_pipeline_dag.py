from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "analytics_team",
    "email_on_failure": True,
    "email": ["alerts@capitaledge.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "capitaledge_financial_pipeline",
    default_args=default_args,
    description="Daily automated financial data pipeline",
    schedule_interval="0 6 * * *",  # runs every day at 6 AM
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id="extract_api_data",
        bash_command="python /opt/airflow/dags/scripts/extract_data.py",
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/dags/scripts/transform_data.py",
    )

    incremental_load_task = BashOperator(
        task_id="incremental_load",
        bash_command="python /opt/airflow/dags/scripts/incremental_load.py",
    )

    extract_task >> transform_task >> incremental_load_task
