import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Best Practices & Airflow 3 Updates:
# 1. Imported PythonOperator from airflow.operators.python (airflow.operators.python_operator is removed).
# 2. Removed deprecated provide_context=True (context is passed automatically).
# 3. Replaced execution_date with logical_date in task execution context.
# 4. Replaced schedule_interval with schedule.
# 5. Replaced dynamic days_ago() with a static pendulum datetime for deterministic scheduling.
# 6. Added default_args with standard retries and retry_delay.


def print_execution_date(**kwargs):
    # Airflow 3 uses logical_date in the task execution context
    print(f"The logical date is: {kwargs.get('logical_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
