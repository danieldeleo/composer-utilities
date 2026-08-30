import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Breaking Changes / Best Practices:
# 1. schedule_interval argument is removed in Airflow 3; use schedule instead.
# 2. airflow.utils.dates.days_ago is removed in Airflow 3; use a fixed static UTC datetime.
# 3. DummyOperator was removed in Airflow 3; use EmptyOperator from airflow.operators.empty.
# 4. Added default_args with retries and retry_delay for resilience.

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
