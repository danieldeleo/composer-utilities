import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)


# Move task generation logic to dynamic task mapping to prevent scheduler parsing bottlenecks
@task
def generate_bash_commands():
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
    # Hook initialization and API calls are deferred to task execution
    # to adhere to top-level code performance constraints.
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
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test"],
) as dag:
    # Optimized from static task generation loop to Airflow Dynamic Task Mapping
    # to significantly reduce DAG parsing time on every scheduler heartbeat.
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

    # Enforce task execution order
    bq_tasks >> check_values >> print_results
