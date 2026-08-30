import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Breaking Changes and Best Practice Updates:
# 1. airflow.operators.python_operator moved to airflow.operators.python.
# 2. provide_context=True in PythonOperator was removed (context is automatically provided).
# 3. execution_date was replaced by logical_date in context kwargs.
# 4. schedule_interval was replaced by schedule.
# 5. Dynamic start_date replaced with a static datetime.
# 6. default_args configured with standard retries and retry_delay.


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date
    logical_date = kwargs.get("logical_date")
    print(f"The logical execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
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
