import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Best Practices & Compatibility Updates:
# 1. schedule parameter is used instead of schedule_interval.
# 2. EmptyOperator is used instead of deprecated/removed DummyOperator.
# 3. Fixed static start_date is used instead of dynamic days_ago.

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
