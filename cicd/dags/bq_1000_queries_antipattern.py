import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="bq_1000_queries_antipattern",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "load_test", "antipattern"],
) as dag:

    # Antipattern: Using a Python loop to statically generate 1000 separate tasks
    # This bloats the DAG definition size and makes the Airflow UI very slow to load
    for i in range(1000):
        BigQueryInsertJobOperator(
            task_id=f"run_select_1_{i}",
            configuration={
                "query": {
                    "query": "SELECT 1",
                    "useLegacySql": False,
                }
            }
        )
