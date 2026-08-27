import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Best Practices:
# 1. Use 'schedule' instead of deprecated 'schedule_interval' (Rule 1 / modern Airflow).
# 2. Use a fixed, static start_date (e.g. pendulum.datetime) instead of dynamic days_ago() to ensure deterministic scheduling (Rule 4).
# 3. Use 'EmptyOperator' from airflow.operators.empty instead of deprecated DummyOperator.
# 4. Configure standard default_args with retries (Rule 6).

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
