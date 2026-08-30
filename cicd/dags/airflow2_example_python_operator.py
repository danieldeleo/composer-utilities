import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Breaking Changes / Best Practices:
# 1. airflow.operators.python_operator was removed in Airflow 3; use airflow.operators.python.
# 2. provide_context=True was removed; context is automatically available via kwargs or TaskFlow.
# 3. execution_date is deprecated/removed in Airflow 3; use logical_date instead.
# 4. schedule_interval is removed in Airflow 3; use schedule instead.
# 5. Dynamic start_date like days_ago() is replaced with a fixed static UTC datetime.
# 6. Added default_args with retries and retry_delay for resilient execution.


def print_execution_date(**kwargs):
    # Retrieve logical_date from context in Airflow 3 (replaces execution_date)
    logical_date = kwargs.get("logical_date")
    print(f"The logical date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
