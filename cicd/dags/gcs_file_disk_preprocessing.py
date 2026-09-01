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
"""Example DAG for preprocessing GCS files using Kubernetes."""

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="gcs_file_disk_preprocessing",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "kubernetes"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def gcs_file_disk_preprocessing():
    @task
    def prepare_file_list():
        # Task returns list of mock GCS file paths to preprocess
        return [
            "gs://example-bucket/raw/data1.csv",
            "gs://example-bucket/raw/data2.csv",
        ]

    @task
    def preprocess_gcs_file(files):
        print(f"Preprocessing GCS files on disk: {files}")

    preprocess_gcs_file(prepare_file_list())


gcs_file_disk_preprocessing()
