# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example DAG demonstrating standard PythonOperator usage across Airflow 2 and 3."""

import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_execution_date(**kwargs):
    # Airflow 3 uses logical_date; fallback to execution_date for backward compatibility
    date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
