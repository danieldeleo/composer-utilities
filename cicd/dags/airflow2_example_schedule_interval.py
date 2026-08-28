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
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="airflow2_example_schedule_interval",
    # Use 'schedule' parameter instead of deprecated 'schedule_interval'
    schedule="@daily",
    # Rule 4: Use a static, fixed start_date instead of dynamic dates like days_ago()
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    # Rule 6: Set default_args with retries and retry_delay
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def airflow2_example_schedule_interval():
    # Airflow 3 uses EmptyOperator from airflow.operators.empty instead of DummyOperator
    start = EmptyOperator(task_id="start_task")
    end = EmptyOperator(task_id="end_task")

    start >> end


airflow2_example_schedule_interval()
