# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest

from datarobot_provider._experimental.example_dags.datarobot_azure_storage_batch_scoring_dag import (
    datarobot_azure_storage_batch_scoring,
)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="datarobot_azure_storage_batch_scoring")
    assert dagbag.import_errors == {}
    # Experimental dags are not loaded
    assert dag is None


def test_dag_structure():
    dag = datarobot_azure_storage_batch_scoring()
    pytest.helpers.assert_dag_dict_equal(
        {
            "get_azure_storage_credentials": ["score_predictions"],
            "score_predictions": ["check_scoring_complete"],
            "check_scoring_complete": [],
        },
        dag,
    )
