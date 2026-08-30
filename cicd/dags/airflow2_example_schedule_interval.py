import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Breaking Changes and Best Practice Updates:
# 1. schedule_interval argument is replaced with schedule.
# 2. airflow.utils.dates.days_ago dynamic date replaced with static datetime.
# 3. DummyOperator from airflow.operators.dummy_operator is replaced with EmptyOperator from airflow.operators.empty.
# 4. default_args configured with standard retries and retry_delay.

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
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
