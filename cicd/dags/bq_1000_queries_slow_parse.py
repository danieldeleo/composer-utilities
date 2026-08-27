import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="bq_1000_queries_slow_parse",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test", "antipattern"],
) as dag:
    # Antipattern: Using a Python loop to statically generate 1000 separate tasks
    # This bloats the DAG definition size and makes the Airflow UI very slow to load
    for i in range(1000):
        emit_number = BashOperator(
            task_id=f"emit_number_{i}",
            bash_command=f"echo {i}",
            do_xcom_push=True,
        )

        run_query = BigQueryInsertJobOperator(
            task_id=f"run_select_{i}",
            configuration={
                "query": {
                    "query": f"SELECT {{{{ ti.xcom_pull(task_ids='emit_number_{i}') }}}}",
                    "useLegacySql": False,
                }
            },
        )

        print_result = BashOperator(
            task_id=f"print_result_{i}",
            bash_command=f"echo {{{{ ti.xcom_pull(task_ids='run_select_{i}') }}}}",
        )

        emit_number >> run_query >> print_result
