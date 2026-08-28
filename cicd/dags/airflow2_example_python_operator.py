import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments to handle transient failures gracefully
default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


def print_execution_date(**kwargs):
    # In modern Airflow (Airflow 2.2+ / Airflow 3), execution_date is replaced by logical_date
    print(f"The logical date is: {kwargs.get('logical_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
