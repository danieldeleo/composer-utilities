import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 3 Breaking Changes demonstrated here:
# 1. airflow.operators.python_operator is removed (moved to airflow.providers.standard.operators.python).
# 2. provide_context=True in PythonOperator is removed (deprecated in Airflow 2).
# 3. execution_date in kwargs is removed (deprecated in Airflow 2, replaced by logical_date).


def print_execution_date(**kwargs):
    # execution_date is no longer passed in Airflow 3; use logical_date instead
    print(f"The execution date is: {kwargs.get('logical_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
