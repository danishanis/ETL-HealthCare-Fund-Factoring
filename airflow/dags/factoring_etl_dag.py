from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="factoring_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,   # manual for now
    catchup=False,
    tags=["factoring", "etl"],
) as dag:

    bootstrap_db = PostgresOperator(
        task_id="bootstrap_db",
        postgres_conn_id="postgres_warehouse",   # we created this in airflow-init
        sql="include/sql/00_bootstrap.sql",
    )

    bootstrap_db

