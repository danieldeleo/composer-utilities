import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="bq_1000_queries",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "load_test"],
) as dag:

    # Generate 1000 configurations for the BigQueryInsertJobOperator
    query_configs = [
        {
            "query": {
                "query": "SELECT 1",
                "useLegacySql": False,
            }
        }
        for _ in range(1000)
    ]

    # Use dynamic task mapping to expand the operator into 1000 tasks
    bq_tasks = BigQueryInsertJobOperator.partial(
        task_id="run_select_1",
    ).expand(configuration=query_configs)
