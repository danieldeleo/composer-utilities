import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Optimized for Airflow 3 compatibility and best practices:
# 1. schedule parameter replaces deprecated schedule_interval
# 2. pendulum.datetime used for static start_date instead of days_ago
# 3. EmptyOperator replaces removed DummyOperator
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
