import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Updates & Best Practices:
# 1. Replaced schedule_interval with schedule (Airflow 3 update).
# 2. Replaced dynamic days_ago start_date with a static, fixed start_date (Rule 4).
# 3. Replaced removed DummyOperator with EmptyOperator from airflow.operators.empty (Airflow 3 update).
# 4. Added default_args with retries and retry_delay for fault tolerance (Rule 6).

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
