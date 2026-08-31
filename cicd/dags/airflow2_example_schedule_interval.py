import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Migration & Best Practice Fixes:
# 1. Replaced removed `schedule_interval` parameter with `schedule`.
# 2. Replaced deprecated/removed `days_ago()` with a fixed, static start_date (Best Practice Rule 4).
# 3. Replaced removed `airflow.operators.dummy_operator.DummyOperator` with `airflow.operators.empty.EmptyOperator`.
# 4. Added standard default_args with retries and retry_delay (Best Practice Rule 6).

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
