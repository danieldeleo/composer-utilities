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
from airflow.operators.empty import EmptyOperator

# Airflow Best Practices applied:
# 1. Use `schedule` argument instead of deprecated `schedule_interval`.
# 2. Use static `start_date` (datetime) instead of dynamic `days_ago`.
# 3. Use `EmptyOperator` from `airflow.operators.empty` instead of deprecated `DummyOperator`.
# 4. Add default_args with retries and retry_delay.
with DAG(
    dag_id="airflow2_example_schedule_interval",
    schedule="@daily",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["airflow2", "compatibility_test"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    start = EmptyOperator(task_id="start_task")

    end = EmptyOperator(task_id="end_task")

    start >> end
