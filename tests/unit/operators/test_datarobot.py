# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import datarobot as dr

import pytest

from datarobot_provider.operators.datarobot import (
    CreateProjectOperator,
    DeployModelOperator,
    DeployRecommendedModelOperator,
    ScorePredictionsOperator,
    TrainModelsOperator,
)


def test_operator_create_project(mocker):
    project_mock = mocker.Mock()
    project_mock.id = "project-id"
    create_project_mock = mocker.patch.object(dr.Project, "create", return_value=project_mock)

    operator = CreateProjectOperator(task_id='create_project')
    project_id = operator.execute(
        context={
            "params": {
                "training_data": "/path/to/s3/or/local/file",
                "project_name": "test project",
                "unsupervised_mode": False,
                "use_feature_discovery": False,
            },
        }
    )

    assert project_id == "project-id"
    create_project_mock.assert_called_with("/path/to/s3/or/local/file", "test project")


def test_operator_train_models(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, "get", return_value=project_mock)

    operator = TrainModelsOperator(task_id="train_models", project_id="project-id")
    settings = {"target": "readmitted"}
    operator.execute(
        context={
            "params": {
                "autopilot_settings": settings
            }
        }
    )

    project_mock.set_target.assert_called_with(**settings)


def test_operator_deploy_model(mocker):
    pred_server_mock = mocker.Mock()
    pred_server_mock.id = "pred-server-id"
    mocker.patch.object(dr.PredictionServer, "list", return_value=[pred_server_mock])
    deployment_mock = mocker.Mock()
    deployment_mock.id = "deployment-id"
    create_mock = mocker.patch.object(dr.Deployment, "create_from_learning_model", return_value=deployment_mock)

    operator = DeployModelOperator(task_id="deploy_model", model_id="model-id")
    deployment_id = operator.execute(
        context={
            "params": {"deployment_label": "test deployment", "deployment_description": "desc"}
        }
    )

    assert deployment_id == "deployment-id"
    create_mock.assert_called_with("model-id", "test deployment", "desc", "pred-server-id")
    deployment_mock.update_drift_tracking_settings.assert_called_with(
        target_drift_enabled=True, feature_drift_enabled=True, max_wait=3600
    )


def test_operator_deploy_recommended_model(mocker):
    model_mock = mocker.Mock()
    model_mock.id = "model-id"
    project_mock = mocker.Mock(target=None)
    project_mock.recommended_model.return_value = model_mock
    mocker.patch.object(dr.Project, "get", return_value=project_mock)
    pred_server_mock = mocker.Mock()
    pred_server_mock.id = "pred-server-id"
    mocker.patch.object(dr.PredictionServer, "list", return_value=[pred_server_mock])
    deployment_mock = mocker.Mock()
    deployment_mock.id = "deployment-id"
    create_mock = mocker.patch.object(dr.Deployment, "create_from_learning_model", return_value=deployment_mock)

    operator = DeployRecommendedModelOperator(task_id="deploy_recommended_model", project_id="project-id")
    deployment_id = operator.execute(
        context={
            "params": {"deployment_label": "test deployment", "deployment_description": "desc"}
        }
    )

    assert deployment_id == "deployment-id"
    create_mock.assert_called_with("model-id", "test deployment", "desc", "pred-server-id")
    deployment_mock.update_drift_tracking_settings.assert_called_with(
        target_drift_enabled=True, feature_drift_enabled=True, max_wait=3600
    )


@pytest.fixture(
    scope="module",
    params=[
        {
            "type": "dataset",
            "dataset_id": "dataset-id",
        },
        {
            "type": "s3",
            "url": "s3://path/to/scoring_dataset.csv",
            "credential_id": "credential-id",
        },
    ],
)
def score_settings(request):
    return {
        "intake_settings": request.param,
        "output_settings": {
            "type": "s3",
            "url": "s3://path/to/predictions.csv",
            "credential_id": "credential-id",
        },
    }


def test_operator_score_predictions(mocker, score_settings):
    job_id = "job-id"
    deployment_id = "deployment-id"

    job_mock = mocker.Mock()
    job_mock.id = job_id
    score_mock = mocker.patch.object(dr.BatchPredictionJob, "score", return_value=job_mock)

    expected_intake_settings = score_settings["intake_settings"].copy()

    if score_settings["intake_settings"]["type"] == "dataset":
        dataset_mock = mocker.Mock()
        dataset_mock.id = "dataset-id"
        mocker.patch.object(dr.Dataset, "get", return_value=dataset_mock)

        del expected_intake_settings["dataset_id"]
        expected_intake_settings["dataset"] = dataset_mock

    operator = ScorePredictionsOperator(
        task_id="score_predictions", deployment_id=deployment_id
    )

    result = operator.execute(
        context={
            "params": {
                "score_settings": score_settings,
            }
        }
    )

    score_mock.assert_called_with(
        deployment_id,
        intake_settings=expected_intake_settings,
        output_settings=score_settings["output_settings"],
    )
    assert result == job_id


def test_operator_score_predictions_fails_when_no_datasetid():
    operator = ScorePredictionsOperator(
        task_id="score_predictions", deployment_id="deployment-id"
    )

    # should raise ValueError if intake type is `dataset` but no dataset_id is supplied
    with pytest.raises(ValueError):
        operator.execute(
            context={
                "params": {
                    "score_settings": {
                        "intake_settings": {
                            "type": "dataset",
                        },
                        "output_settings": {
                            "type": "s3",
                            "url": "s3://path/to/predictions.csv",
                            "credential_id": "credential-id",
                        },
                    },
                }
            }
        )
