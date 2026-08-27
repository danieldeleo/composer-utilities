import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_execution_date(**kwargs):
    # In modern Airflow, logical_date replaces the deprecated execution_date
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    # Rule 4: Start dates must be fixed and static rather than dynamic
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    # provide_context=True is deprecated and unnecessary in modern Airflow
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
