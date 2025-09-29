from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="check_files_dag",
    start_date=days_ago(1),
    schedule_interval=None,  # manual for now
    catchup=False,
    tags=["sanity", "debug"],
) as dag:

    list_files = BashOperator(
        task_id="list_csv_files",
        bash_command="ls -l /opt/data/input", # lists all the mounted files
    )

    preview_bank = BashOperator(
        task_id="preview_bank_transactions_csv",
        bash_command="head -n 5 /opt/data/input/bank_transactions.csv", # preview first 5 lines of bank_transactions.csv
    )

    list_files >> preview_bank