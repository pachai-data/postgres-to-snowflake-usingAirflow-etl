from datetime import timedelta
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="postgres_to_snowflake_etl",
    description="Extract PostgreSQL orders, clean data, and load to Snowflake",
    start_date=pendulum.datetime(2026, 10, 15, tz="Asia/Kolkata"),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=6),
    },
    tags=["postgresql", "python", "snowflake"],
) as dag:


    run_etl = BashOperator(
        task_id="run_postgres_to_snowflake_etl",
        bash_command=(
            "cd /home/pacha/airflow_project && "
            "/home/pacha/airflow_project/airflow_venv/bin/python "
            "script/etl_pipeline.py"
        ),
        execution_timeout=timedelta(minutes=5),
    )
