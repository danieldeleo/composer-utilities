import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_execution_date(**kwargs):
    # Airflow 3 replaces execution_date with logical_date
    logical_date = kwargs.get("logical_date")
    print(f"The logical date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["airflow3", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
