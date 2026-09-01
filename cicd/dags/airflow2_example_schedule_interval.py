import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

# Airflow 3 Migrations & Best Practices:
# 1. Replaced schedule_interval with schedule.
# 2. Replaced dynamic days_ago with fixed static start_date.
# 3. Replaced deprecated DummyOperator with EmptyOperator from standard provider.
# 4. Added default_args with standard retries and retry_delay.
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
