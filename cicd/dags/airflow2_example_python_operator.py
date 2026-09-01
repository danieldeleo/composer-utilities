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

from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow Best Practices applied:
# 1. Use `airflow.operators.python.PythonOperator` instead of deprecated `airflow.operators.python_operator`.
# 2. Context is automatically passed in Airflow 2/3 (provide_context removed).
# 3. Use `logical_date` instead of deprecated `execution_date`.
# 4. Use static start_date (datetime) instead of dynamic days_ago.
# 5. Use `schedule` parameter instead of deprecated `schedule_interval`.
# 6. Add default_args with retries and retry_delay.


def print_execution_date(**kwargs):
    # Retrieve logical_date from task execution context
    print(f"The logical date is: {kwargs.get('logical_date')}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
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
