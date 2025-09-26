from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook # To insert data into postgres
from airflow.utils.dates import days_ago
import json

with DAG(
    dag_id='etl_healthcare_factoring_postgres',
    start_date=days_ago(1), # scheduling daily run
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Step 1: Create a table if it does not exist

    @task
    def create_table():
        # initializing Postgreshook (to interact with Postgres SQL)
        postgres_hook = PostgresHook(
            postgre_conn_id="my_postgres_connection"
        )

        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_facilities (
            facility_id SERIAL PRIMARY KEY,
            counter_party_name VARCHAR(255),
            max_advance_rate NUMERIC(5, 4),
            reserve_rate NUMERIC(5, 4),
            concentration_limit_pct NUMERIC(5, 4),
            delinquency_cutoff_days INTEGER,
            current_outstanding NUMERIC(15, 2),
            currency VARCHAR(3)
        );

        CREATE TABLE IF NOT EXISTS policy_commissions (
            policy_id VARCHAR(50) PRIMARY KEY,
            facility_id VARCHAR(50) REFERENCES raw_facilities(facility_id),
            insurer VARCHAR(100),
            insured_name VARCHAR(100),
            policy_number VARCHAR(50),
            commission_due_date DATE,
            commission_amount NUMERIC(10, 2),
            written_date DATE,
            status VARCHAR(20),
            currency VARCHAR(3),
            expected_bank_memo TEXT
        );


        CREATE TABLE IF NOT EXISTS raw_bank_transactions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50),
            amount NUMERIC,
            transaction_date DATE
        );

        """
        
        # Executing table creation query
        postgres_hook.run(create_table_query)


    # Step 2: Extract date from source (Could be an API or repository folder)


    # Step 3: Transform data


    # Step 4: Loading data into Postgres SQL


    # Step 5: Verify data with DBViewer


    # Step 6: Define task dependencies