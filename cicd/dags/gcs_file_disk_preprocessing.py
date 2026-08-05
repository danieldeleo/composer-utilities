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
"""
An example DAG that uses KubernetesPodOperator to process a file from GCS.
"""

import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Define default parameters for the DAG
# These can be overridden at trigger time by changing the value in the Airflow UI when triggering the DAG.
GCS_BUCKET = "your-gcs-bucket-name"  # <--- CHANGE THIS
INPUT_OBJECT = "path/to/your/input_file.txt"  # <--- CHANGE THIS
OUTPUT_OBJECT = "path/to/your/processed_file.txt"  # <--- CHANGE THIS


@dag(
    dag_id="gcs_file_disk_preprocessing",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "kubernetes", "example"],
    # Added default_args to ensure all tasks gracefully handle transient failures per best practice rule 6.
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "gcs_bucket": Param(GCS_BUCKET, type="string", title="GCS Bucket"),
        "input_object": Param(INPUT_OBJECT, type="string", title="Input Object"),
        "output_object": Param(OUTPUT_OBJECT, type="string", title="Output Object"),
    },
)
def gcs_file_disk_preprocessing():
    """
    ### GCS File on Disk Preprocessing DAG

    This DAG demonstrates how to use the `KubernetesPodOperator` to download a file
    from Google Cloud Storage to large local disk, process it, and upload the result back to GCS.

    """

    # The bash script to be executed in the pod.
    # It uses Jinja templating to access the DAG's parameters.
    bash_script = """
    set -eo pipefail
    INPUT_GCS_BUCKET="{{ params.gcs_bucket }}"

    gcloud storage rsync "gs://${INPUT_GCS_BUCKET}/" /mnt/ephemeral_volume/

    echo "Disk processing complete. Job finished."
    """

    # Persistent Volume Claim must be used to get storage sizes > 10Gi on GKE
    # Otherwise emptyDir is limited to 10Gi
    # https://docs.cloud.google.com/kubernetes-engine/docs/how-to/generic-ephemeral-volumes#storage-types
    volume = k8s.V1Volume(
        name="ephemeral-volume",
        ephemeral=k8s.V1EphemeralVolumeSource(
            volume_claim_template=k8s.V1PersistentVolumeClaimTemplate(
                spec=k8s.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    storage_class_name="standard-rwo",
                    resources=k8s.V1VolumeResourceRequirements(
                        requests={"storage": "1Ti"}
                    ),
                )
            )
        ),
    )

    volume_mount = k8s.V1VolumeMount(
        name="ephemeral-volume",
        mount_path="/mnt/ephemeral_volume",
        sub_path=None,
        read_only=False,
    )

    KubernetesPodOperator(
        task_id="process_gcs_file",
        name="gcs-file-disk-processor-pod",
        namespace="composer-user-workloads",
        image="gcr.io/google.com/cloudsdktool/cloud-sdk:latest",
        cmds=["bash"],
        arguments=["-c", bash_script],
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        log_events_on_failure=True,
        do_xcom_push=False,
        volumes=[volume],
        volume_mounts=[volume_mount],
    )


# Instantiate the DAG
gcs_file_disk_preprocessing()
