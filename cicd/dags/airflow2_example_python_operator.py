import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Best Practices & Compatibility Updates:
# 1. PythonOperator is imported from airflow.operators.python.
# 2. provide_context parameter is removed in Airflow 3.
# 3. logical_date is used instead of the deprecated execution_date.
# 4. schedule is used instead of deprecated schedule_interval.
# 5. Fixed static start_date is used instead of dynamic dates like days_ago.


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date in context
    print(f"The execution date is: {kwargs.get('logical_date')}")


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
