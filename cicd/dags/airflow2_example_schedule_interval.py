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
from airflow.operators.empty import EmptyOperator

# Airflow 3 Modernizations and Best Practices:
# 1. 'schedule_interval' is replaced by 'schedule' in Airflow 3.
# 2. 'airflow.utils.dates.days_ago' is removed in Airflow 3; static start_date used (Best Practice 4).
# 3. 'DummyOperator' is removed in Airflow 3; replaced with 'EmptyOperator' from 'airflow.operators.empty'.
# 4. Added standard default_args with retries and retry_delay (Best Practice 6).

with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
