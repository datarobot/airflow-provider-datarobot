# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datarobot_provider.example_dags.datarobot_create_project_from_ai_catalog_dag import (
    create_project_from_aicatalog,
)
from tests.unit.dags.conftest import assert_dag_dict_equal


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="create_project_from_aicatalog")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2


def test_dag_structure():
    dag = create_project_from_aicatalog()
    assert_dag_dict_equal(
        {"dataset_uploading": ["create_project"], "create_project": []},
        dag,
    )
