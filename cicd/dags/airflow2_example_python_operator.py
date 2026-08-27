from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Airflow 3 Breaking Changes demonstrated here:
# 1. airflow.operators.python_operator is removed (moved to airflow.operators.python in Airflow 2).
# 2. provide_context=True in PythonOperator is removed (deprecated in Airflow 2).
# 3. execution_date in kwargs is removed (deprecated in Airflow 2, replaced by logical_date).


def print_execution_date(**kwargs):
    # execution_date is no longer passed in Airflow 3
    print(f"The execution date is: {kwargs.get('execution_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
        provide_context=True,
    )
