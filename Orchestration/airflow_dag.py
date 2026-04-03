from datetime import datetime, timedelta
import os

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.getenv(
    "PROJECT_DIR", "/Users/mohanjawahar/gitrepo/module2project")
DATA_DIR = os.getenv(
    "DATA_DIR", "/Users/mohanjawahar/DataScience/data/module2data")
DBT_DIR = os.getenv(
    "DBT_DIR", f"{PROJECT_DIR}")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", DBT_DIR)


default_args = {
    "owner": "ecommerce-dag",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_dag",
    default_args=default_args,
    description="Ingest Olist data, run dbt on BigQuery, execute data quality tests, and analysis",
    schedule=None,
    catchup=False,
    tags=["ecommerce", "elt", "dbt", "bigquery"],
) as dag:

    BashOperator(
        task_id="echo_ok",
        bash_command='echo "Airflow works"'
    )

    ingest = BashOperator(
        task_id="ingest_raw_data_bigquery",
        bash_command=f"cd {PROJECT_DIR} && python loaddataset.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    data_quality = BashOperator(
        task_id="custom_data_quality",
        bash_command=f"cd {PROJECT_DIR} && python tests/rundataquality.py --project-id pilot-488720 --dataset ecommerce_marts --location US 2>&1",
    )

    analytics = BashOperator(
        task_id="run_analysis",
        bash_command=f"cd {PROJECT_DIR} && python analysis/eda.py",
    )


ingest >> dbt_run >> dbt_test >> data_quality >> analytics
