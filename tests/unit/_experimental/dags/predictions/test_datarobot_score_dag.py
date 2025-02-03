# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest

from datarobot_provider._experimental.example_dags.datarobot_score_dag import datarobot_score


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="datarobot_score")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2


def test_dag_structure():
    dag = datarobot_score()
    pytest.helpers.assert_dag_dict_equal(
        {
            "score_predictions": ["check_scoring_complete"],
            "check_scoring_complete": [],
        },
        dag,
    )
