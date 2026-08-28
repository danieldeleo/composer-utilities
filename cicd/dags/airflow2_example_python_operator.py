from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 updates:
# 1. airflow.operators.python_operator moved to airflow.operators.python.
# 2. provide_context=True removed (context is automatically available or passed).
# 3. execution_date replaced by logical_date.
# 4. schedule_interval replaced with schedule, and static start_date used.


def print_execution_date(**kwargs):
    # In Airflow 3, use logical_date instead of execution_date
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
