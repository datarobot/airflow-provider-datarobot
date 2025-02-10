# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest

from datarobot_provider._experimental.example_dags.datarobot_pipeline_dag import datarobot_pipeline


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="datarobot_pipeline")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 8


def test_dag_structure():
    dag = datarobot_pipeline()
    pytest.helpers.assert_dag_dict_equal(
        {
            "create_project": [
                "train_models",
                "check_autopilot_complete",
                "deploy_recommended_model",
            ],
            "train_models": ["check_autopilot_complete"],
            "check_autopilot_complete": ["deploy_recommended_model"],
            "deploy_recommended_model": [
                "feature_drift",
                "score_predictions",
                "target_drift",
            ],
            "score_predictions": ["check_scoring_complete"],
            "check_scoring_complete": ["target_drift", "feature_drift"],
            "target_drift": [],
            "feature_drift": [],
        },
        dag,
    )
