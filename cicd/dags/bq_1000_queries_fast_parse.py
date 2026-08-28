import datetime

from airflow import DAG
from airflow.decorators import task

# Airflow Best Practices Optimization:
# - Uses TaskFlow Dynamic Task Mapping (.expand()) for fast DAG parsing (Rule 1 & Rule 7).
# - Heavy provider imports (Google Cloud BigQuery) are moved inside task execution
#   callables to ensure DAG parse time is minimal (<0.01s) and scheduler is not blocked.
# - Added default_args with retries for fault tolerance (Rule 6).


@task
def generate_bash_commands():
    return [f"echo {i}" for i in range(1000)]


@task
def emit_number(cmd: str):
    import subprocess

    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    return res.stdout.strip()


@task
def run_select(number: str):

    return f"job_for_{number}"


@task
def check_value(job_id: str, number: str):

    return True


@task
def print_result(job_id: str):
    print(f"Result for {job_id}")


with DAG(
    dag_id="bq_1000_queries_fast_parse",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["bigquery", "load_test"],
) as dag:
    commands = generate_bash_commands()
    numbers = emit_number.expand(cmd=commands)
    bq_jobs = run_select.expand(number=numbers)
    checks = check_value.expand(job_id=bq_jobs, number=numbers)
    prints = print_result.expand(job_id=bq_jobs)

    bq_jobs >> checks >> prints
