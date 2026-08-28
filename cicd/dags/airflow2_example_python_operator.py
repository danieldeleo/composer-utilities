import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Updates & Best Practices:
# 1. Replaced deprecated airflow.operators.python_operator with airflow.operators.python.
# 2. Removed provide_context=True which is removed in Airflow 3 (context is automatically provided).
# 3. Use logical_date instead of removed execution_date.
# 4. Replaced dynamic start_date (days_ago) with a fixed, static start_date (Rule 4).
# 5. Replaced schedule_interval with schedule (Airflow 3 update).
# 6. Added default_args with retries and retry_delay for fault tolerance (Rule 6).


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date
    dag_run = kwargs.get("dag_run")
    logical_date = kwargs.get("logical_date") or (
        dag_run.logical_date if dag_run else None
    )
    print(f"The logical date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
