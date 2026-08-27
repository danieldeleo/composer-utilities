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

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
)
def airflow2_example_python_operator():
    """
    DAG migrated to Airflow 3 and optimized using TaskFlow API.
    - Uses TaskFlow @dag and @task decorators for clean, idiomatic execution.
    - Uses fixed static start_date to prevent scheduler drift.
    - Uses schedule instead of deprecated schedule_interval.
    - Uses logical_date from runtime context instead of removed execution_date.
    """

    @task(task_id="print_execution_date_task")
    def print_execution_date(**kwargs):
        # In Airflow 3, execution_date is replaced by logical_date in task context
        logical_date = kwargs.get("logical_date")
        print(f"The execution date is: {logical_date}")

    print_execution_date()


# Instantiate the DAG
airflow2_example_python_operator()
