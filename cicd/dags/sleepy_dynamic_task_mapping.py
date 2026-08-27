import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=100,
    default_args={
        "retries": 10,
        "retry_delay": datetime.timedelta(seconds=10),
    },
)
def sleepy_dynamic_task_mapping():
    @task
    def get_sleepy_arguments():
        return [
            [
                "-c",
                rf"""
                set -e && \
                echo "Try number: $AIRFLOW_RETRY_NUMBER" && \
                echo "Sleeping for {minutes} minutes" && \
                sleep {minutes}m
                """,
            ]
            for minutes in [1, 1, 1, 1, 1]
        ]

    KubernetesPodOperator.partial(
        task_id="sleepy_pod",
        name="sleepy",
        cmds=["bash"],
        env_vars={"AIRFLOW_RETRY_NUMBER": "{{ task_instance.try_number }}"},
        namespace="composer-user-workloads",
        image="gcr.io/google.com/cloudsdktool/google-cloud-cli:latest",
        config_file="/home/airflow/composer_kube_config",
        kubernetes_conn_id="kubernetes_default",
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "100m",
                "memory": "64Mi",
            },
            limits={
                "cpu": "100m",
                "memory": "64Mi",
            },
        ),
    ).expand(arguments=get_sleepy_arguments())


# Instantiate the DAG
sleepy_dynamic_task_mapping()
