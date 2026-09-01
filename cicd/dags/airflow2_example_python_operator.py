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

# Best Practice & Compatibility Updates:
# 1. Imports updated from airflow.operators.python_operator to airflow.operators.python.
# 2. Replaced dynamic start_date (days_ago) with a fixed, static start_date (Rule 4).
# 3. Replaced deprecated schedule_interval with schedule parameter.
# 4. Removed deprecated provide_context=True.
# 5. Updated kwargs execution_date access to logical_date for Airflow 3 compatibility.
# 6. Added standard default_args for retries and retry_delay (Rule 6).


def print_execution_date(**kwargs):
    # execution_date is replaced by logical_date in modern Airflow
    print(f"The execution date is: {kwargs.get('logical_date')}")


default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["airflow2", "compatibility_test", "optimized"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
