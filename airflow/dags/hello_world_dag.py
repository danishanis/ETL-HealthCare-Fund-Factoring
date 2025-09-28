from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,   # run manually
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello, Airflow!"'
    )

    hello_task
