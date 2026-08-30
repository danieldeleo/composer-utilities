from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# Airflow 3 Breaking Changes demonstrated here:
# 1. schedule_interval argument is removed in Airflow 3. Use schedule instead.
# 2. airflow.utils.dates.days_ago is removed in Airflow 3.
# 3. DummyOperator from airflow.operators.dummy_operator is removed in Airflow 3.
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = DummyOperator(task_id="start_task")

    end = DummyOperator(task_id="end_task")

    start >> end
