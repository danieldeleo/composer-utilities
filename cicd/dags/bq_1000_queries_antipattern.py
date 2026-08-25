import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


@task
def generate_query_configs():
    # Move heavy computations/task generation outside of the top-level Python scope.
    # Refactored to use dynamic task mapping instead of top-level loop.
    return [
        {
            "query": {
                "query": "SELECT 1",
                "useLegacySql": False,
            }
        }
        for _ in range(1000)
    ]


with DAG(
    dag_id="bq_1000_queries_antipattern",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test", "optimized"],
) as dag:
    query_configs = generate_query_configs()

    bq_tasks = BigQueryInsertJobOperator.partial(
        task_id="run_select_1",
    ).expand(configuration=query_configs)
