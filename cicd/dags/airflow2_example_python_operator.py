import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Breaking Changes & Best Practices:
# 1. airflow.operators.python_operator is removed (moved to airflow.operators.python).
# 2. provide_context=True in PythonOperator is removed (deprecated in Airflow 2).
# 3. execution_date in kwargs is removed (replaced by logical_date).
# 4. schedule_interval is replaced with schedule.
# 5. days_ago is replaced with a fixed static start_date.
# 6. default_args configured with retries and retry_delay.

default_args = {
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date in task context
    print(f"The execution date is: {kwargs.get('logical_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
