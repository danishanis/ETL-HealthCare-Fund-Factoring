from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import os

from ingestion import load_csv_postgres
from exports import export_marts_to_csv

from spark_transforms import run_spark_bank_demo

import pendulum

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="factoring_etl",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 9, 1, tz="America/Toronto"),
    schedule_interval="0 7 * * *",  # daily at 7am
    catchup=False, # to avoid backfill
    tags=["factoring", "etl"],
    template_searchpath=["/opt/airflow/include/sql"],
) as dag:

    # Step 1: Bootstrap the DB (create schemas and empty tables)
    bootstrap_db = PostgresOperator(
        task_id="bootstrap_db",
        postgres_conn_id="postgres_warehouse",
        sql="00_bootstrap.sql",
    )

    # Step 2: Load raw CSVs into raw schema tables

    load_bank = PythonOperator(
        task_id="load_bank_transactions",
        python_callable=load_csv_postgres,
        op_args=["bank_transactions", "bank_transactions.csv"],
    )

    load_policy = PythonOperator(
        task_id="load_policy_commissions",
        python_callable=load_csv_postgres,
        op_args=["policy_commissions", "policy_commissions.csv"],
    )

    load_facilities = PythonOperator(
        task_id="load_facilities",
        python_callable=load_csv_postgres,
        op_args=["facilities", "facilities.csv"],
    )

    # Step 3: Transform raw in staging layer
    transform_staging = PostgresOperator(
        task_id="transform_staging",
        postgres_conn_id="postgres_warehouse",
        sql="01_staging.sql",
    )

    # Step 4: Transform staging to core/marts
    transform_core = PostgresOperator(
        task_id="transform_core",
        postgres_conn_id="postgres_warehouse",
        sql="02_core.sql",
    )

    # Step 5: Defining Marts transformations
    transform_marts = PostgresOperator(
        task_id="transform_marts",
        postgres_conn_id="postgres_warehouse",
        sql="03_marts.sql",
    )

    # Step 6: Export outputs
    export_outputs = PythonOperator(
        task_id="export_outputs",
        python_callable=export_marts_to_csv,
    )

    # Spark Transformation Task
    spark_demo = PythonOperator(
        task_id="spark_bank_demo",
        python_callable=run_spark_bank_demo,
    )

    # DAG Dependencies
    bootstrap_db >> [load_bank, load_policy, load_facilities] >> \
        transform_staging >> transform_core >> transform_marts >> \
        export_outputs

    # Spark Transformation Task
    bootstrap_db >> spark_demo