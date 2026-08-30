import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date in context
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


# Best practices: Use fixed static start_date, schedule parameter instead of schedule_interval,
# and define standard default_args with retries.
default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test"],
) as dag:
    # provide_context=True was removed in Airflow 3 as context is passed automatically
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
