import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="bq_1000_queries_antipattern",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test", "antipattern"],
) as dag:
    # Use dynamic task mapping instead of a static Python loop to avoid
    # blowing up the DAG parsing time and Airflow UI.
    query_configs = [
        {
            "query": {
                "query": "SELECT 1",
                "useLegacySql": False,
            }
        }
        for _ in range(1000)
    ]

    BigQueryInsertJobOperator.partial(
        task_id="run_select_1",
    ).expand(configuration=query_configs)
