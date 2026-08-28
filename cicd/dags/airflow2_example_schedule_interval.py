import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Best Practices Applied:
# 1. Start dates are fixed and static (Rule 4).
# 2. Replaced deprecated schedule_interval with schedule.
# 3. Replaced deprecated DummyOperator with EmptyOperator.
# 4. Added comprehensive default_args with retries and retry_delay (Rule 6).
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
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
