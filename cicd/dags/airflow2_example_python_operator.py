import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Migration & Best Practice Fixes:
# 1. Replaced removed `airflow.operators.python_operator` with `airflow.operators.python`.
# 2. Replaced deprecated/removed `days_ago()` with a fixed, static start_date (Best Practice Rule 4).
# 3. Replaced removed `schedule_interval` with `schedule`.
# 4. Removed removed `provide_context=True` parameter (unnecessary in Airflow 2/3).
# 5. Access `logical_date` from context kwargs instead of deprecated `execution_date`.
# 6. Added standard default_args with retries and retry_delay (Best Practice Rule 6).


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date in context
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


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
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
