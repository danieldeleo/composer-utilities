import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Fixed antipattern: Moved heavy loop generation to task execution context using dynamic task mapping.
# This prevents the DAG file from bloating and keeps scheduler performance fast.
@task
def generate_bash_commands():
    return [f"echo {i}" for i in range(1000)]


@task
def make_bq_config(number: str):
    return {
        "query": {
            # Use jinja templating/XCom natively within the execution context
            "query": f"SELECT {number}",
            "useLegacySql": False,
        }
    }


@task
def generate_print_commands(job_id: str):
    return f"echo {job_id}"


with DAG(
    dag_id="bq_1000_queries_antipattern",
    schedule_interval=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test", "antipattern"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    bash_commands = generate_bash_commands()

    emit_number = BashOperator.partial(task_id="emit_number", do_xcom_push=True).expand(
        bash_command=bash_commands
    )

    bq_configs = make_bq_config.expand(number=emit_number.output)

    run_query = BigQueryInsertJobOperator.partial(
        task_id="run_select",
    ).expand(configuration=bq_configs)

    print_commands = generate_print_commands.expand(job_id=run_query.output)

    print_result = BashOperator.partial(
        task_id="print_result",
    ).expand(bash_command=print_commands)
