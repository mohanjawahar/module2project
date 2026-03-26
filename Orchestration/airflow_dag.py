from datetime import datetime, timedelta
import os

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.getenv(
    "PROJECT_DIR", "/Users/mohanjawahar")
DATA_DIR = os.getenv(
    "DATA_DIR", f"{PROJECT_DIR}/DataScience/data/module2data")
DBT_DIR = os.getenv(
    "DBT_DIR", f"{PROJECT_DIR}/gitrepo/module2project")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", DBT_DIR)


default_args = {
    "owner": "dag-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_elt_bigquery_pipeline",
    default_args=default_args,
    description="Ingest Olist data, run dbt on BigQuery, execute data quality tests, and analysis",
    start_date=datetime(2025, 1, 1),
    schedule="None",
    catchup=False,
    tags=["ecommerce", "elt", "dbt", "bigquery"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data_bigquery",
        bash_command=f"cd {PROJECT_DIR} && python /gitrepo/module2project/loaddataset.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )
'''
    data_quality = BashOperator(
        task_id="custom_data_quality",
        bash_command=f"cd {PROJECT_DIR} && python tests/run_data_quality_bigquery.py",
    )

    analytics = BashOperator(
        task_id="run_analysis",
        bash_command=f"cd {PROJECT_DIR} && python analysis/eda_bigquery.py",
    )
'''
ingest >> dbt_run >> dbt_test
