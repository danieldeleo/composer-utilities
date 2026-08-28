import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_execution_date(**kwargs):
    # In Airflow 2.2+ and Airflow 3, logical_date represents the execution timestamp.
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


# Best Practices Applied:
# 1. Start dates are fixed and static (Rule 4).
# 2. Replaced deprecated schedule_interval with schedule.
# 3. Replaced deprecated airflow.operators.python_operator with airflow.operators.python.
# 4. Removed deprecated provide_context=True (unneeded since Airflow 2.0).
# 5. Added comprehensive default_args with retries and retry_delay (Rule 6).
with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
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
