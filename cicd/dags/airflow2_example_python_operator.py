import datetime

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
)
def airflow2_example_python_operator():
    @task(task_id="print_execution_date_task")
    def print_execution_date(**kwargs):
        # Best Practice: In Airflow 3+, 'logical_date' replaces the legacy 'execution_date'
        logical_date = kwargs.get("logical_date")
        print(f"The logical date is: {logical_date}")

    print_execution_date()


airflow2_example_python_operator()
