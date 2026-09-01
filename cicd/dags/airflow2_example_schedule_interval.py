import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

# Airflow 3 Breaking Changes resolved:
# 1. schedule_interval replaced with schedule argument
# 2. airflow.utils.dates.days_ago replaced with static pendulum datetime
# 3. DummyOperator replaced with EmptyOperator from airflow.providers.standard.operators.empty

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
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
