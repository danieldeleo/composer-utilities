import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Optimized: Replaced static 1000-task loop with Dynamic Task Mapping to prevent DAG parsing timeouts.
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


with DAG(
    dag_id="bq_1000_queries_antipattern",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
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

    print_commands = generate_print_commands.expand(job_id=bq_tasks.output)

    print_results = BashOperator.partial(
        task_id="print_result",
    ).expand(bash_command=print_commands)
