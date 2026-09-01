import datetime

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


@task
def generate_bash_commands():
    # Best Practice: Generate inputs dynamically at execution time rather than creating thousands of static tasks at DAG parse time
    return [f"echo {i}" for i in range(1000)]


@task
def make_bq_config(number: str):
    return {
        "query": {
            "query": f"SELECT {number}",
            "useLegacySql": False,
        }
    }


@task
def generate_print_commands(job_id: str):
    return f"echo {job_id}"


@task
def make_check_kwargs(job_id: str, number: str):
    # Best Practice: Retrieve GCP BigQuery client and metadata within task execution context
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook()
    client = hook.get_client()
    job = client.get_job(job_id)
    dest = job.destination
    table_id = f"{dest.project}.{dest.dataset_id}.{dest.table_id}"

    return {
        "sql": f"SELECT * FROM `{table_id}`",
        "pass_value": int(number.strip()),
    }


with DAG(
    dag_id="bq_1000_queries_slow_parse",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["bigquery", "load_test", "optimized"],
) as dag:
    bash_commands = generate_bash_commands()

    emit_number = BashOperator.partial(task_id="emit_number", do_xcom_push=True).expand(
        bash_command=bash_commands
    )

    bq_configs = make_bq_config.expand(number=emit_number.output)

    bq_tasks = BigQueryInsertJobOperator.partial(
        task_id="run_select",
    ).expand(configuration=bq_configs)

    check_kwargs = make_check_kwargs.expand(
        job_id=bq_tasks.output, number=emit_number.output
    )

    check_values = BigQueryValueCheckOperator.partial(
        task_id="check_value",
        use_legacy_sql=False,
    ).expand_kwargs(check_kwargs)

    print_commands = generate_print_commands.expand(job_id=bq_tasks.output)

    print_results = BashOperator.partial(
        task_id="print_result",
    ).expand(bash_command=print_commands)

    bq_tasks >> check_values >> print_results
