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
import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    # Rule 4: Fixed static start date instead of dynamic days_ago()
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    # Rule 6: Comprehensive default_args definition with standard retries
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
)
def airflow2_example_python_operator():
    # Rule 7: TaskFlow API adoption (@task decorator) replaces deprecated PythonOperator patterns
    @task(task_id="print_execution_date_task")
    def print_execution_date(**kwargs):
        # In modern Airflow, logical_date replaces deprecated execution_date
        logical_date = kwargs.get("logical_date")
        print(f"The execution date is: {logical_date}")

    print_execution_date()


airflow2_example_python_operator()
