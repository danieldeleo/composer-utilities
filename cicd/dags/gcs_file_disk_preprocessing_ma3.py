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
An example DAG that uses KubernetesPodOperator to process a file from GCS in Managed Airflow (Composer) v3.
"""

import pendulum
import yaml
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from kubernetes.client import models as k8s

# Define default parameters for the DAG
# These can be overridden at trigger time by changing the value in the Airflow UI when triggering the DAG.
GCS_BUCKET = "your-gcs-bucket-name"  # <--- CHANGE THIS
INPUT_OBJECT = "path/to/your/input_file.txt"  # <--- CHANGE THIS
OUTPUT_OBJECT = "path/to/your/processed_file.txt"  # <--- CHANGE THIS


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "kubernetes", "example"],
    params={
        "gcs_bucket": Param(GCS_BUCKET, type="string", title="GCS Bucket"),
        "input_object": Param(INPUT_OBJECT, type="string", title="Input Object"),
        "output_object": Param(OUTPUT_OBJECT, type="string", title="Output Object"),
    },
)
def gcs_file_disk_preprocessing_ma3():
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

    pvc_name = "gcs-file-disk-processor-pvc-{{ ts_nodash | lower }}"

    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "namespace": "composer-user-workloads",
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "standard-rwo",
            "resources": {"requests": {"storage": "1Ti"}},
        },
    }

    create_pvc = KubernetesCreateResourceOperator(
        task_id="create_pvc",
        yaml_conf=yaml.dump(pvc_manifest),
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
    )

    volume = k8s.V1Volume(
        name="ephemeral-volume",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name=pvc_name
        ),
    )

    volume_mount = k8s.V1VolumeMount(
        name="ephemeral-volume",
        mount_path="/mnt/ephemeral_volume",
        sub_path=None,
        read_only=False,
    )

    process_gcs_file = KubernetesPodOperator(
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

    delete_pvc = KubernetesDeleteResourceOperator(
        task_id="delete_pvc",
        yaml_conf=yaml.dump(pvc_manifest),
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        trigger_rule="all_done",
    )

    create_pvc >> process_gcs_file >> delete_pvc


# Instantiate the DAG
gcs_file_disk_preprocessing_ma3()
