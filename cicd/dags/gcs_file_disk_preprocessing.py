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
"""DAG demonstrating GCS file disk preprocessing using KubernetesPodOperator."""

import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


@dag(
    dag_id="gcs_file_disk_preprocessing",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "kubernetes"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def gcs_file_disk_preprocessing():
    @task
    def prepare_preprocessing_spec():
        """Prepares preprocessing configuration specification."""
        return {"batch_size": 100, "status": "ready"}

    preprocess_k8s_task = KubernetesPodOperator(
        task_id="preprocess_file_on_disk",
        name="gcs-file-preprocessing",
        cmds=["bash", "-c"],
        arguments=["echo 'Preprocessing GCS file data on disk...' && exit 0"],
        namespace="composer-user-workloads",
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:latest",
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "100m",
                "memory": "64Mi",
            }
        ),
    )

    prepare_preprocessing_spec() >> preprocess_k8s_task


gcs_file_disk_preprocessing()
