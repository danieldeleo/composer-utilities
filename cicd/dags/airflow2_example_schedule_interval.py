import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Airflow 3 Compatibility & Best Practices:
# 1. schedule_interval argument is removed in Airflow 3. Use schedule instead.
# 2. airflow.utils.dates.days_ago is removed in Airflow 3. Use static pendulum start_date.
# 3. DummyOperator from airflow.operators.dummy_operator is removed in Airflow 3. Use EmptyOperator.
# 4. default_args configured with retries and retry_delay.

default_args = {
    "retries": 3,
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
