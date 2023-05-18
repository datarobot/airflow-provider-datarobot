# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datarobot_provider.example_dags.datarobot_gcp_storage_batch_scoring_dag import (
    datarobot_gcp_batch_scoring,
)
from tests.unit.dags.conftest import assert_dag_dict_equal


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="datarobot_gcp_batch_scoring")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3


def test_dag_structure():
    dag = datarobot_gcp_batch_scoring()
    assert_dag_dict_equal(
        {
            "get_gcp_credentials": ["score_predictions"],
            "score_predictions": ["check_scoring_complete"],
            "check_scoring_complete": [],
        },
        dag,
    )
