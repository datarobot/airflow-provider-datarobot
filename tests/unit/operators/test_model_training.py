# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.model_training import TrainModelOperator


def test_operator_train_model(mocker):
    project_id = "test-project-id"
    blueprint_id = "test-blueprint-id"
    featurelist_id = "test-featurelist-id"
    source_project_id = "test-source-project-id"
    job_id = 123

    project_mock = mocker.Mock()
    project_mock.id = project_id
    project_mock.max_train_rows = 100

    get_project_mock = mocker.patch.object(dr.Project, "get", return_value=project_mock)

    blueprint_mock = mocker.Mock()
    blueprint_mock.id = blueprint_id
    blueprint_mock.project_id = project_id

    get_blueprint_mock = mocker.patch.object(dr.Blueprint, "get", return_value=blueprint_mock)

    request_train_model_mock = mocker.patch.object(project_mock, "train", return_value=job_id)

    operator = TrainModelOperator(
        task_id="train_model",
        project_id=project_id,
        blueprint_id=blueprint_id,
        featurelist_id=featurelist_id,
        source_project_id=source_project_id,
    )

    result = operator.execute(context={"params": {}})

    get_project_mock.assert_called_with(project_id)
    get_blueprint_mock.assert_called_with(project_id, blueprint_id)
    request_train_model_mock.assert_called_with(
        blueprint_mock,
        sample_pct=None,
        featurelist_id=featurelist_id,
        source_project_id=source_project_id,
        scoring_type=None,
        training_row_count=None,
        n_clusters=None,
    )
    assert result == job_id


def test_operator_train_model_no_project_id():
    project_id = None
    blueprint_id = "test-blueprint-id"

    operator = TrainModelOperator(
        task_id="train_model", project_id=project_id, blueprint_id=blueprint_id
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_train_model_no_blueprint_id():
    project_id = "test-project-id"
    blueprint_id = None

    operator = TrainModelOperator(
        task_id="train_model", project_id=project_id, blueprint_id=blueprint_id
    )

    with pytest.raises(ValueError):
        operator.validate()
