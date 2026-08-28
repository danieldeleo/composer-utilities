import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Best Practices & Airflow 3 Updates:
# 1. DummyOperator from airflow.operators.dummy_operator is replaced by EmptyOperator from airflow.operators.empty.
# 2. schedule_interval argument is replaced by schedule.
# 3. Dynamic start_date days_ago() is replaced with static pendulum datetime for deterministic scheduling.
# 4. Added default_args with standard retries and retry_delay.
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
