import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 3 Migrations & Best Practices:
# 1. Migrated airflow.operators.python_operator to airflow.providers.standard.operators.python.
# 2. Removed deprecated provide_context=True (context is passed automatically).
# 3. Replaced deprecated execution_date with logical_date.
# 4. Replaced schedule_interval with schedule and dynamic days_ago with fixed static start_date.
# 5. Added default_args with standard retries and retry_delay.


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date is used instead of execution_date
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
