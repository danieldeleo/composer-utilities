import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Best Practices:
# 1. Import PythonOperator from 'airflow.operators.python' (Rule 1).
# 2. provide_context=True is deprecated/removed in Airflow 2/3 as context is passed automatically.
# 3. Use 'logical_date' instead of deprecated 'execution_date' (Airflow 2/3 compatibility).
# 4. Use a fixed, static start_date (Rule 4).
# 5. Use 'schedule' instead of 'schedule_interval'.
# 6. Configure standard default_args with retries (Rule 6).

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def print_execution_date(**kwargs):
    # Use logical_date to retrieve execution timestamp from task context
    logical_date = kwargs.get("logical_date")
    print(f"The execution date is: {logical_date}")


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
