import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Optimized for Airflow 3 compatibility and best practices:
# 1. airflow.operators.python replaces deprecated airflow.operators.python_operator
# 2. provide_context=True removed (default in modern Airflow)
# 3. logical_date replaces execution_date in task context
# 4. pendulum.datetime used for static start_date
# 5. schedule parameter replaces schedule_interval


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
