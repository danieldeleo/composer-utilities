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
"""Optimized BigQuery queries DAG demonstrating fast parse times using TaskFlow and dynamic mapping."""

import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="bq_1000_queries_slow_parse",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    tags=["bigquery", "load_test", "optimized"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def bq_1000_queries_slow_parse():
    # Optimized: Using TaskFlow API and Dynamic Task Mapping with deferred provider imports.
    # Moving provider imports into execution contexts eliminates scheduler parse bottlenecks.
    @task
    def generate_numbers():
        """Generates numbers dynamically at execution time."""
        return list(range(100))

    @task
    def execute_bq_query(number: int):
        """Executes query within task execution context avoiding top-level imports."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook()
        client = hook.get_client()
        query_job = client.query(f"SELECT {number}")
        return {"job_id": query_job.job_id, "number": number}

    @task
    def validate_and_print(query_info: dict):
        """Validates query result and prints output."""
        print(f"Validated query output: {query_info}")

    numbers = generate_numbers()
    query_results = execute_bq_query.expand(number=numbers)
    validate_and_print.expand(query_info=query_results)


bq_1000_queries_slow_parse()
