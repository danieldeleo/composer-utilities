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
"""Example of a Composer DAG that runs a long-running (5min) KubernetesPodOperator with retries."""

import datetime

import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def sleepy_kubernetes_pod_operator():
    KubernetesPodOperator(
        task_id="sleep",
        cmds=["bash"],
        arguments=[
            "-c",
            r"""
            set -e && \
            echo "Try number: $AIRFLOW_RETRY_NUMBER" && \
            echo "Sleeping for 5 minutes" && \
            sleep 5m
            """,
        ],
        env_vars={"AIRFLOW_RETRY_NUMBER": "{{ task_instance.try_number }}"},
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:latest",
        namespace="composer-user-workloads",
        # Specifies path to kubernetes config. The config_file is templated.
        config_file="/home/airflow/composer_kube_config",
        # Identifier of connection that should be used
        kubernetes_conn_id="kubernetes_default",
    )


sleepy_kubernetes_pod_operator()
