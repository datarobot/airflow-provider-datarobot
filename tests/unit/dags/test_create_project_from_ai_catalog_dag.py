# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest
from airflow.models import DagBag

from datarobot_provider.example_dags.datarobot_create_project_from_ai_catalog_dag import (
    create_project_from_aicatalog,
)


@pytest.fixture()
def dagbag(provider_dir):
    return DagBag(dag_folder=f"{str(provider_dir)}/example_dags", include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="create_project_from_aicatalog")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2


def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag_structure():
    dag = create_project_from_aicatalog()
    assert_dag_dict_equal(
        {"dataset_uploading": ["create_project"], "create_project": []},
        dag,
    )
