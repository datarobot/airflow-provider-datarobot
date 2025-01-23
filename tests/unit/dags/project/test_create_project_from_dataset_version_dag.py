# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest

from datarobot_provider.example_dags.deprecated.datarobot_create_project_from_dataset_version_dag import (
    create_project_from_dataset_version,
)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="create_project_from_dataset_version")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2


def test_dag_structure():
    dag = create_project_from_dataset_version()
    pytest.helpers.assert_dag_dict_equal(
        {"dataset_new_version": ["create_project"], "create_project": []},
        dag,
    )
