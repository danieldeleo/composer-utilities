from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 updates:
# 1. schedule replaces schedule_interval.
# 2. EmptyOperator replaces DummyOperator.
# 3. Static start_date is used instead of days_ago.
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
