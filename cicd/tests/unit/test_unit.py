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
import sys
import unittest.mock
from pathlib import Path

import pytest
from airflow.models import DagBag

PARSING_DURATION_THRESHOLD = 2.0


@pytest.fixture(scope="session")
def dagbag():
    dags_path = str((Path(__file__).parent.parent.parent / "dags").resolve())
    sys.path.insert(0, dags_path)
    import airflow  # Used only to check version for backward compatibility

    kwargs = {"include_examples": False} if airflow.__version__.startswith("2.") else {}
    yield DagBag(dag_folder=dags_path, **kwargs)


def test_dagbag_not_empty(dagbag):
    assert dagbag.size() > 0, "Dagbag should not be empty."


def test_dagbag_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, "No import errors should be found."


""" Uncomment below if you want to fail on warnings
def test_dagbag_no_import_warnings(dagbag):
    assert len(dagbag.captured_warnings) == 0, "No warnings should be found."
"""


def test_filename_matches_dag_id(dagbag):
    """Tests that filename matches dag_id"""
    for dag in dagbag.dags.values():
        assert dag.dag_id == Path(dag.relative_fileloc).stem, (
            "Filename does not match DAG ID."
        )


def test_all_dags_have_start_date(dagbag):
    """Tests that all DAGs have a start_date."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.start_date is not None, f"DAG {dag_id} does not have a start_date."


def test_gcs_file_disk_preprocessing(dagbag):
    dag = dagbag.get_dag("gcs_file_disk_preprocessing")
    assert dag is not None, "DAG gcs_file_preprocessing not found."
    assert len(dag.tasks) == 1, "DAG gcs_file_preprocessing should contain 1 task."
    assert not dag.catchup, "DAG gcs_file_preprocessing should have catchup=False."


def test_sleepy_dynamic_task_mapping_structure(dagbag):
    dag = dagbag.get_dag("sleepy_dynamic_task_mapping")
    assert dag is not None
    # get_sleepy_minutes and the expanded task group
    # Note: in TaskFlow, the number of tasks might be different depending on how they are counted
    assert len(dag.tasks) >= 2


def test_sleepy_kubernetes_pod_operator_structure(dagbag):
    dag = dagbag.get_dag("sleepy_kubernetes_pod_operator")
    assert dag is not None
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "sleep"


def test_sleepy_task_group_structure(dagbag):
    dag = dagbag.get_dag("sleepy_task_group")
    assert dag is not None
    assert len(dag.tasks) >= 3


def test_dagbag_parse_times(dagbag):
    """Test that each DAG file takes less than the threshold to parse."""
    failing_dags = []
    for stat in dagbag.dagbag_stats:
        duration = (
            stat.duration.total_seconds()
            if hasattr(stat.duration, "total_seconds")
            else stat.duration
        )
        file_path = stat.file if hasattr(stat, "file") else stat.fileloc
        if duration >= PARSING_DURATION_THRESHOLD:
            failing_dags.append(
                f"DAG parsing for {file_path} took too long: {duration} seconds (threshold: {PARSING_DURATION_THRESHOLD}s)"
            )

    assert not failing_dags, "\n".join(failing_dags)


def test_dag_tags_example(dagbag):
    """Example of testing that a specific DAG has the expected tags."""
    dag = dagbag.get_dag("gcs_file_disk_preprocessing")
    assert dag is not None, "DAG gcs_file_disk_preprocessing not found."
    assert "gcs" in dag.tags
    assert "kubernetes" in dag.tags


def test_dag_default_args_example(dagbag):
    """Example of testing that a DAG has specific default_args configured."""
    dag = dagbag.get_dag("sleepy_kubernetes_pod_operator")
    assert dag is not None, "DAG sleepy_kubernetes_pod_operator not found."
    assert "retries" in dag.default_args
    assert dag.default_args["retries"] >= 3


def test_task_properties_example(dagbag):
    """Example of testing specific properties of a task."""
    dag = dagbag.get_dag("sleepy_kubernetes_pod_operator")
    assert dag is not None, "DAG sleepy_kubernetes_pod_operator not found."

    # Retrieve the specific task
    task = dag.get_task("sleep")

    # Check properties
    assert task.image == "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest"
    assert task.namespace == "composer-user-workloads"


def test_kubernetes_pod_operator_namespace(dagbag):
    """Tests that any KubernetesPodOperator uses the 'composer-user-workloads' namespace."""
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    invalid_tasks = []
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            if (
                isinstance(task, KubernetesPodOperator)
                and getattr(task, "namespace", None) != "composer-user-workloads"
            ):
                invalid_tasks.append(
                    f"DAG: {dag_id}, Task: {task.task_id}, Namespace: {getattr(task, 'namespace', None)}"
                )

    assert not invalid_tasks, (
        "The following KubernetesPodOperator tasks do not use the 'composer-user-workloads' namespace:\n"
        + "\n".join(invalid_tasks)
    )


def test_dag_params_example(dagbag):
    """Example of testing that a DAG has expected parameters defined."""
    dag = dagbag.get_dag("gcs_file_disk_preprocessing")
    assert dag is not None, "DAG gcs_file_disk_preprocessing not found."

    assert "gcs_bucket" in dag.params
    assert "input_object" in dag.params
    assert "output_object" in dag.params


def test_dag_cycle_example(dagbag):
    """Example of explicitly testing a DAG for cycles (circular dependencies)."""
    from airflow.utils.dag_cycle_tester import check_cycle

    dag = dagbag.get_dag("sleepy_kubernetes_pod_operator")
    assert dag is not None, "DAG sleepy_kubernetes_pod_operator not found."

    # Will raise AirflowDagCycleException if a cycle is found
    check_cycle(dag)


# List of DAG IDs inferred from directory listing
DAG_IDS = [
    "gcs_file_disk_preprocessing",
    "sleepy_dynamic_task_mapping",
    "sleepy_kubernetes_pod_operator",
    "sleepy_task_group",
]


# Fixture to apply common mocks for all execution tests
@pytest.fixture(autouse=True)
def common_mocks():
    import datetime

    with (
        unittest.mock.patch(
            "airflow.models.Variable.get", return_value="mock_value", autospec=True
        ),
        unittest.mock.patch(
            "airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator.execute",
            return_value=None,
            autospec=True,
        ),
        unittest.mock.patch(
            "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.execute",
            return_value=None,
            autospec=True,
        ),
        unittest.mock.patch(
            "airflow.providers.google.cloud.hooks.gcs.GCSHook.exists",
            return_value=True,
            autospec=True,
        ),
        unittest.mock.patch(
            "airflow.providers.google.cloud.hooks.gcs.GCSHook.get_size",
            return_value=123,
            autospec=True,
        ),
        unittest.mock.patch(
            "airflow.providers.google.cloud.hooks.gcs.GCSHook.get_conn", autospec=True
        ) as mock_get_conn,
        unittest.mock.patch(
            "google.cloud.orchestration.airflow.service_v1.EnvironmentsClient.save_snapshot",
            autospec=True,
        ) as mock_save,
    ):
        # Mock GCS client and bucket for trigger_snapshot task
        mock_client = unittest.mock.MagicMock()
        mock_bucket = unittest.mock.MagicMock()
        mock_blob = unittest.mock.MagicMock()
        mock_blob.name = "snapshots/project_id_location_env_2026-03-31T/airflow-database.postgres.sql.gz"
        mock_blob.time_created = datetime.datetime.now(datetime.timezone.utc)
        mock_bucket.list_blobs.return_value = [mock_blob]
        mock_client.bucket.return_value = mock_bucket
        mock_get_conn.return_value = mock_client

        # Configure mock_save to return something with a snapshot_path attribute
        mock_operation = unittest.mock.MagicMock()
        mock_operation.result.return_value = unittest.mock.MagicMock(
            snapshot_path="gs://mock/path"
        )
        mock_save.return_value = mock_operation
        yield


# @pytest.mark.parametrize("dag_id", DAG_IDS)
# def test_dag_execution(dagbag, dag_id):
#     """Runs dag.test() for each DAG with common mocks applied."""
#     dag = dagbag.get_dag(dag_id)
#     assert dag is not None, f"DAG {dag_id} not found in DagBag."
#     try:
#         dag.test()
#     except Exception as e:
#         print(f"Warning: dag.test() failed for {dag_id}: {e}")
#         raise e


# def pytest_collection_modifyitems(config, items):
#     """Moves tests starting with 'test_dag_execution' to the end of the list and prints the order."""
#     items.sort(key=lambda item: item.name.startswith("test_dag_execution"))
#     print("\n[Pytest Order of Execution]:")
#     for item in items:
#         print(f"  {item.nodeid}")
