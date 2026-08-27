import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Best Practice (Rule 1 & 8): Move heavy task generation out of top-level code into
# dynamic task mapping (expand) so the DAG parser does not evaluate 1000s of task definitions
# on every scheduler heartbeat.
default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}


@task
def generate_bash_commands():
    """Generates the bash commands dynamically at runtime."""
    return [f"echo {i}" for i in range(1000)]


@task
def make_bq_config(number: str):
    """Constructs BigQuery query configuration at runtime."""
    return {
        "query": {
            "query": f"SELECT {number}",
            "useLegacySql": False,
        }
    }


@task
def generate_print_commands(job_id: str):
    """Constructs print commands for BigQuery execution output."""
    return f"echo {job_id}"


with DAG(
    dag_id="bq_1000_queries_slow_parse",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "load_test", "optimized"],
) as dag:
    bash_commands = generate_bash_commands()

    emit_number = BashOperator.partial(
        task_id="emit_number",
        do_xcom_push=True,
    ).expand(bash_command=bash_commands)

    bq_configs = make_bq_config.expand(number=emit_number.output)

    bq_tasks = BigQueryInsertJobOperator.partial(
        task_id="run_select",
    ).expand(configuration=bq_configs)

    print_commands = generate_print_commands.expand(job_id=bq_tasks.output)

    print_results = BashOperator.partial(
        task_id="print_result",
    ).expand(bash_command=print_commands)
