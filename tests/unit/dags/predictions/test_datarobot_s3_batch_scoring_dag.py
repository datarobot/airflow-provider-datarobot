# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest

from datarobot_provider.example_dags.datarobot_aws_s3_batch_scoring_dag import (
    datarobot_s3_batch_scoring,
)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id='datarobot_s3_batch_scoring')
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3


def test_dag_structure():
    dag = datarobot_s3_batch_scoring()
    pytest.helpers.assert_dag_dict_equal(
        {
            'get_aws_credentials': ['score_predictions'],
            'score_predictions': ['check_scoring_complete'],
            'check_scoring_complete': [],
        },
        dag,
    )
