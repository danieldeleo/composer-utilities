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
from airflow.decorators import dag, task


@dag(
    dag_id="airflow2_example_python_operator",
    # Use 'schedule' instead of deprecated 'schedule_interval' for Airflow 2.4+ / Airflow 3
    schedule="@daily",
    # Rule 4: Use a static, fixed start_date to prevent shifting start times
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    # Rule 6: Set default_args with retries and retry_delay
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def airflow2_example_python_operator():
    # Rule 7: Adopt TaskFlow API (@task) for clean execution and context handling
    @task(task_id="print_execution_date_task")
    def print_execution_date(**kwargs):
        # 'logical_date' replaces the removed 'execution_date' in Airflow 3
        logical_date = kwargs.get("logical_date")
        print(f"The execution date is: {logical_date}")

    print_execution_date()


airflow2_example_python_operator()
