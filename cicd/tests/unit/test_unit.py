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

PARSING_DURATION_THRESHOLD = 10.0


@pytest.fixture(scope="session")
def dagbag():
    dags_path = str((Path(__file__).parent.parent.parent / "dags").resolve())
    sys.path.insert(0, dags_path)
    return DagBag(dag_folder=dags_path, include_examples=False)


def test_dagbag_not_empty(dagbag):
    assert dagbag.size() > 0, "Dagbag should not be empty."


def test_dagbag_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"


def test_filename_matches_dag_id(dagbag):
    """Tests that filename matches dag_id"""
    for dag in dagbag.dags.values():
        assert dag.dag_id == Path(dag.relative_fileloc).stem, (
            f"Filename {dag.relative_fileloc} does not match DAG ID {dag.dag_id}."
        )


def test_all_dags_have_start_date(dagbag):
    """Tests that all DAGs have a start_date."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.start_date is not None, f"DAG {dag_id} does not have a start_date."


def test_all_dags_have_retries(dagbag):
    """Tests that all DAGs have retries set in default_args."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("retries") is not None, (
            f"DAG {dag_id} does not have retries set."
        )


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


# Execution tests with mocks
@pytest.fixture(autouse=True)
def common_mocks():
    with (
        unittest.mock.patch(
            "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.execute",
            return_value=None,
            autospec=True,
        ),
    ):
        yield


def test_gcs_file_disk_preprocessing_structure(dagbag):
    dag = dagbag.get_dag("gcs_file_disk_preprocessing")
    assert dag is not None
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "process_gcs_file"


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
