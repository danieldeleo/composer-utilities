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
"""Example DAG demonstrating GCS preprocessing using KubernetesPodOperator."""

import datetime

import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    dag_id="gcs_file_disk_preprocessing",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "kubernetes"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def gcs_file_disk_preprocessing():
    KubernetesPodOperator(
        task_id="preprocess_gcs_files",
        name="gcs-file-disk-preprocessing",
        namespace="composer-user-workloads",
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:latest",
        cmds=["bash", "-cx"],
        arguments=["echo Preprocessing GCS files..."],
    )


gcs_file_disk_preprocessing()
