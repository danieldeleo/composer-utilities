import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Best practices: Use fixed static start_date, schedule parameter instead of schedule_interval,
# EmptyOperator instead of deprecated DummyOperator, and default_args with retries.
default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
