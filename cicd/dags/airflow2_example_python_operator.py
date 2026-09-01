import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 3 Breaking Changes resolved:
# 1. airflow.operators.python_operator moved to airflow.providers.standard.operators.python
# 2. provide_context=True removed (context is automatically available)
# 3. execution_date replaced by logical_date
# 4. schedule_interval replaced with schedule
# 5. Dynamic start_date replaced with static pendulum datetime


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces the deprecated execution_date
    logical_date = kwargs.get("logical_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
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
