import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def print_execution_date(**kwargs):
    # execution_date is replaced by logical_date in Airflow 3
    logical_date = kwargs.get("logical_date", kwargs.get("execution_date"))
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
