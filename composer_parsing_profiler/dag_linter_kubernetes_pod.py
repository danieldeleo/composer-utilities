# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START composer_dag_parsing_profiler_dag]
"""
Orchestration DAG for the Composer Parsing Profiler.

This script defines the Airflow DAG that provisions and launches the isolated
profiling environment. It handles:
1. Configuration resolution (auto-detecting buckets and worker images).
2. Resource provisioning (ephemeral storage volumes).
3. Execution of the core analysis logic within a KubernetesPodOperator.
"""

from __future__ import annotations

import json
import os
import pendulum
import requests

import google.auth
from google.auth.transport.requests import Request

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowSkipException
from kubernetes.client import models as k8s

# ==============================================================================
# ⚙️ CONFIGURATION SECTION
# ==============================================================================

# --- 1. Feature Flags ---
_CONFIG_PROFILE_SLOW_DAGS = True
_CONFIG_FETCH_DATA_AND_PLUGINS = True
_CONFIG_PROFILE_SORT_KEY = "tottime"
_CONFIG_PARSE_TIME_THRESHOLD_SECONDS = 1.0

# --- 2. Infrastructure Configuration ---
_CONFIG_GCS_BUCKET_NAME = None
_CONFIG_POD_IMAGE = None

# --- 3. Source Folder Paths (Defaults) ---
_CONFIG_GCS_DAGS_SOURCE_FOLDER = "dags/"
_CONFIG_GCS_PLUGINS_SOURCE_FOLDER = "plugins/"
_CONFIG_GCS_DATA_SOURCE_FOLDER = "data/"

# --- 4. Kubernetes Resources ---
_CONFIG_POD_DISK_SIZE = "10Gi"
_CONFIG_POD_NAMESPACE = "composer-user-workloads"
_CONFIG_POD_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "4000m", "memory": "16Gi"},
    limits={"cpu": "4000m", "memory": "16Gi"},
)


def _verify_docker_image_v2(image_uri: str) -> bool:
    """Verifies if an image exists by checking its manifest via the Docker Registry V2 API."""
    print(f"   🔎 Verifying Manifest for: {image_uri}")
    try:
        parts = image_uri.split('/')
        domain = parts[0]
        repo_path = "/".join(parts[1:])
        api_url = f"https://{domain}/v2/{repo_path}/manifests/latest"
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request())
        response = requests.get(
            api_url,
            headers={'Authorization': f'Bearer {credentials.token}'},
            timeout=10
        )
        if response.status_code == 200:
            print(f"   ✅ Manifest found (HTTP 200). Valid Custom Image.")
            return True
        return False
    except requests.RequestException:
        return False


def _get_c3_image_string() -> str | None:
    """Constructs and verifies the C3 image URL."""
    fingerprint = os.getenv('COMPOSER_OPERATION_FINGERPRINT')
    project_id = os.getenv('GCP_TENANT_PROJECT')
    location = os.getenv('COMPOSER_LOCATION')
    if not all([fingerprint, location, project_id]):
        return None
    image_uuid = fingerprint.split('@')[0]
    registry_domain = f"{location}-docker.pkg.dev"
    c3_image = f"{registry_domain}/{project_id}/composer-images/{image_uuid}"
    if _verify_docker_image_v2(c3_image):
        return c3_image
    return None


def _get_c2_image_api() -> str | None:
    """Queries the Google Artifact Registry API to find the latest Docker image (C2)."""
    project_id = os.getenv('GCP_PROJECT')
    location = os.getenv('COMPOSER_LOCATION')
    gke_name = os.getenv('COMPOSER_GKE_NAME')
    if not all([project_id, location, gke_name]):
        return None
    repo_name = f"composer-images-{gke_name}"
    try:
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request())
    except google.auth.exceptions.DefaultCredentialsError:
        return None
    api_url = (
        f"https://artifactregistry.googleapis.com/v1/"
        f"projects/{project_id}/locations/{location}/repositories/{repo_name}/dockerImages"
    )
    try:
        response = requests.get(
            api_url,
            params={'pageSize': 1, 'orderBy': 'update_time desc'},
            headers={'Authorization': f'Bearer {credentials.token}'},
            timeout=10
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        if 'dockerImages' in data and len(data['dockerImages']) > 0:
            return data['dockerImages'][0]['uri'].split('@')[0]
        return None
    except requests.RequestException:
        return None


@dag(
    dag_id="composer_dag_parser_profile",
    start_date=pendulum.datetime(2025, 8, 6, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["profiler", "troubleshooting", "gcp-composer"],
    doc_md=__doc__,
)
def composer_dag_parser_profile():
    @task(retries=0)
    def detect_worker_image_task() -> str:
        """Determines the correct worker image to use for the profiler Pod."""
        if _CONFIG_POD_IMAGE:
            return _CONFIG_POD_IMAGE
        c3_fingerprint = os.getenv('COMPOSER_OPERATION_FINGERPRINT')
        if c3_fingerprint:
            image = _get_c3_image_string()
            if image:
                return image
        else:
            image = _get_c2_image_api()
            if image:
                return image
        raise AirflowSkipException("No Custom Image found.")

    @task
    def prepare_linter_script_task() -> str:
        """Reads the core script, resolves configuration, and builds the python command."""
        target_bucket = _CONFIG_GCS_BUCKET_NAME or os.environ.get("GCS_BUCKET")
        if not target_bucket:
            raise ValueError("Could not auto-detect GCS Bucket.")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        core_script_path = os.path.join(current_dir, 'linter_core.py')
        with open(core_script_path, 'r') as f:
            linter_script_content = f.read()

        linter_config_payload = {
            "GCS_BUCKET": target_bucket,
            "BASE_WORK_DIR": "/mnt/data/airflow_content",
            "FETCH_EXTRAS": _CONFIG_FETCH_DATA_AND_PLUGINS,
            "PROFILE_SLOW": _CONFIG_PROFILE_SLOW_DAGS,
            "PROFILE_SORT_KEY": _CONFIG_PROFILE_SORT_KEY,
            "PARSE_TIME_THRESHOLD_SECONDS": _CONFIG_PARSE_TIME_THRESHOLD_SECONDS,
            "GCS_DAGS_SOURCE_FOLDER": _CONFIG_GCS_DAGS_SOURCE_FOLDER,
            "GCS_PLUGINS_SOURCE_FOLDER": _CONFIG_GCS_PLUGINS_SOURCE_FOLDER,
            "GCS_DATA_SOURCE_FOLDER": _CONFIG_GCS_DATA_SOURCE_FOLDER,
        }
        return linter_script_content + f"\nmain('{json.dumps(linter_config_payload)}')"

    # Infrastructure components
    storage_volume = k8s.V1Volume(
        name="ephemeral-storage",
        empty_dir=k8s.V1EmptyDirVolumeSource(size_limit=_CONFIG_POD_DISK_SIZE),
    )
    storage_volume_mount = k8s.V1VolumeMount(
        name="ephemeral-storage",
        mount_path="/mnt/data",
    )

    image_uri = detect_worker_image_task()
    linter_command = prepare_linter_script_task()

    KubernetesPodOperator(
        task_id="profile_and_check_linter",
        name="dag-linter-pod",
        namespace=_CONFIG_POD_NAMESPACE,
        image=image_uri,
        arguments=["-c", linter_command],
        cmds=["python"],
        container_resources=_CONFIG_POD_RESOURCES,
        do_xcom_push=False,
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=300,
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        volumes=[storage_volume],
        volume_mounts=[storage_volume_mount],
    )


composer_dag_parser_profile()
# [END composer_dag_parsing_profiler_dag]
