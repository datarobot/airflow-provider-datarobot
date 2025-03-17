# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from unittest.mock import MagicMock
from unittest.mock import patch

import datarobot as dr
import pytest

from datarobot_provider.operators.model_training import AdvancedTuneModelOperator
from datarobot_provider.operators.model_training import GetTrainedModelParametersOperator
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


@pytest.mark.parametrize(
    "parameters, set_parameter_calls",
    [
        ([], 0),
        ([("task_name", "parameter_name", "value")], 1),
        (
            [
                ("task_name1", "parameter_name1", "value1"),
                ("task_name2", "parameter_name2", "value2"),
            ],
            2,
        ),
    ],
)
@patch("datarobot_provider.operators.model_training.dr.Model.get")
def test_advanced_tune_model_operator_execute(mock_get_model, parameters, set_parameter_calls):
    mock_model = MagicMock()
    mock_tune = MagicMock()
    mock_job = MagicMock()
    mock_job.id = "job-id"
    mock_tune.run.return_value = mock_job
    mock_model.start_advanced_tuning_session.return_value = mock_tune
    mock_get_model.return_value = mock_model

    operator = AdvancedTuneModelOperator(
        task_id="advanced_tune", project_id="project-id", model_id="model-id", parameters=parameters
    )
    result = operator.execute(context={})
    assert result == "job-id"
    mock_get_model.assert_called_once_with("project-id", "model-id")
    assert mock_tune.set_parameter.call_count == set_parameter_calls
    for call in parameters:
        mock_tune.set_parameter.assert_any_call(
            task_name=call[0], parameter_name=call[1], value=call[2]
        )
    mock_tune.run.assert_called_once()


@pytest.mark.parametrize(
    "project_id, model_id, expected_exception, match",
    [
        ("project-id", "model-id", None, None),
        (None, "model-id", ValueError, "project_id is required."),
        ("project-id", None, ValueError, "model_id is required."),
    ],
)
def test_get_model_parameters_operator_validate(project_id, model_id, expected_exception, match):
    operator = GetTrainedModelParametersOperator(
        task_id="get_model_parameters", project_id=project_id, model_id=model_id
    )
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.model_training.dr.Model.get")
def test_get_model_parameters_operator_execute(mock_get_model):
    mock_model = MagicMock()
    mock_parameters = MagicMock()
    mock_parameters.parameters = {"param1": "value1", "param2": "value2"}
    mock_model.get_parameters.return_value = mock_parameters
    mock_get_model.return_value = mock_model

    operator = GetTrainedModelParametersOperator(
        task_id="get_model_parameters", project_id="project-id", model_id="model-id"
    )
    result = operator.execute(context={})
    assert result == {"param1": "value1", "param2": "value2"}
    mock_get_model.assert_called_once_with("project-id", "model-id")
    mock_model.get_parameters.assert_called_once()
