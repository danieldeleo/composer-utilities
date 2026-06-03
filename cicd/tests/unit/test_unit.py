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
from pendulum.tz.timezone import Timezone

PARSING_DURATION_THRESHOLD = 10.0


@pytest.fixture(scope="session")
def dagbag():
    dags_path = str((Path(__file__).parent.parent.parent / "dags").resolve())
    sys.path.insert(0, dags_path)
    yield DagBag(dag_folder=dags_path, include_examples=False)


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


def test_sleepy_dag(dagbag):
    dag = dagbag.get_dag("sleepy")
    assert dag is not None, "DAG sleepy not found."
    assert len(dag.tasks) == 3, "DAG sleepy should contain 3 tasks."


# def test_custom_task_group_example(dagbag):
#     dag = dagbag.get_dag("custom_task_group_example")
#     assert dag is not None, "DAG custom_task_group_example not found."
#     assert len(dag.tasks) == 4, "DAG custom_task_group_example should contain 4 tasks."


def test_timezone_aware_dag(dagbag):
    dag = dagbag.get_dag("gcs_object_existence_sensor_test")
    assert dag.timezone == Timezone("America/New_York"), (
        "DAG timezone should be America/New_York."
    )


def test_bq_query_dag_test_date(dagbag):
    dag = dagbag.get_dag("bq_query_dag_test_date")
    assert dag is not None, "DAG bq_query_dag_test_date not found."
    assert len(dag.tasks) == 1, "DAG bq_query_dag_test_date should contain 1 task."
    assert not dag.catchup, "DAG bq_query_dag_test_date should have catchup=False."


def test_airflow_db_export(dagbag):
    dag = dagbag.get_dag("airflow_db_export")
    assert dag is not None, "DAG airflow_db_export not found."
    assert set(dag.tags) == {
        "airflow_db",
        "bigquery",
        "export",
        "upsert",
        "snapshots",
    }, "DAG airflow_db_export tags mismatch."
    assert not dag.catchup, "DAG airflow_db_export should have catchup=False."


def test_gcs_file_preprocessing(dagbag):
    dag = dagbag.get_dag("gcs_file_preprocessing")
    assert dag is not None, "DAG gcs_file_preprocessing not found."
    assert len(dag.tasks) == 1, "DAG gcs_file_preprocessing should contain 1 task."
    assert not dag.catchup, "DAG gcs_file_preprocessing should have catchup=False."


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


# List of DAG IDs inferred from directory listing
DAG_IDS = [
    "airflow_db_export",
    "bq_query_dag_test_date",
    "circular_conf_achilles_heel",
    "composer_dag_sensor_bad_example",
    "composer_dag_sensor_example_dynamic",
    "custom_cloud_composer_dag_run_sensor",
    "custom_cloud_composer_trigger_dag_run_operator_example",
    "custom_composer_external_task_sensor_example",
    "custom_external_task_sensor_example",
    "custom_parallel_task_group_1k",
    "custom_parallel_task_group_example",
    "custom_sequential_task_group_example",
    "custom_sleepy_task_group_example",
    "dag_triggerer",
    "dynamic_task_group_nos",
    "dynamic_task_group_tpt",
    "dynamic_task_tpt",
    "gcs_file_disk_preprocessing",
    "gcs_file_preprocessing",
    "gcs_object_existence_sensor_test",
    "sleepy",
    "sleepy_pod",
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
