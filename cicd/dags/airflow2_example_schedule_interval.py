import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

# Airflow 3 Breaking Changes demonstrated here:
# 1. schedule_interval argument is removed in Airflow 3. Use schedule instead.
# 2. airflow.utils.dates.days_ago is removed in Airflow 3. Use pendulum/datetime with static date.
# 3. DummyOperator from airflow.operators.dummy_operator is removed in Airflow 3. Use EmptyOperator.
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
