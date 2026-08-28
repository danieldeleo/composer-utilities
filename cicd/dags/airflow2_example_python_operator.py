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
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow 3 Modernizations and Best Practices:
# 1. 'airflow.operators.python_operator' is removed; imported from 'airflow.operators.python'.
# 2. 'provide_context=True' in PythonOperator is removed in Airflow 3; context is passed automatically.
# 3. 'execution_date' in task context is replaced by 'logical_date' / 'data_interval_start'.
# 4. 'schedule_interval' is replaced by 'schedule' parameter in Airflow 3.
# 5. Dynamic start date 'days_ago()' is replaced with a static start_date (Best Practice 4).
# 6. Added standard default_args with retries and retry_delay (Best Practice 6).


def print_execution_date(**kwargs):
    # In Airflow 3, logical_date replaces execution_date in kwargs context
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    print(f"The execution date is: {logical_date}")


with DAG(
    dag_id="airflow2_example_python_operator",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    print_date = PythonOperator(
        task_id="print_execution_date_task",
        python_callable=print_execution_date,
    )
