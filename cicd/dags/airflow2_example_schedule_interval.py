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
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["airflow2", "compatibility_test"],
)
def airflow2_example_schedule_interval():
    """
    DAG migrated to Airflow 3 and optimized following best practices:
    - Uses EmptyOperator instead of removed DummyOperator.
    - Uses schedule parameter instead of removed schedule_interval.
    - Uses fixed static start_date (pendulum) instead of dynamic days_ago.
    - Includes default_args with retry policy.
    """
    start = EmptyOperator(task_id="start_task")
    end = EmptyOperator(task_id="end_task")

    start >> end


# Instantiate the DAG
airflow2_example_schedule_interval()
