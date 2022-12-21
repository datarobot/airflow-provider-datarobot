# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import datarobot as dr

from datarobot_provider.operators.datarobot import (
    CreateProjectOperator,
    DeployModelOperator,
    DeployRecommendedModelOperator,
    ScorePredictionsOperator,
    TrainModelsOperator,
    GetFeatureDriftOperator,
    GetTargetDriftOperator,
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


def test_operator_score_predictions(mocker):
    job_mock = mocker.Mock()
    job_mock.id = "job-id"
    score_mock = mocker.patch.object(dr.BatchPredictionJob, "score", return_value=job_mock)

    operator = ScorePredictionsOperator(task_id="score_predictions", deployment_id="deployment-id")
    settings = {
        "intake_settings": {
            "type": "s3",
            "url": "s3://path/to/scoring_dataset.csv",
            "credential_id": "credential-id",
        },
        "output_settings": {
            "type": "s3",
            "url": "s3://path/to/predictions.csv",
            "credential_id": "credential-id",
        },
    }
    job_id = operator.execute(
        context={
            "params": {
                "score_settings": settings,
            }
        }
    )

    assert job_id == "job-id"
    score_mock.assert_called_with("deployment-id", **settings)


def test_operator_get_target_drift(mocker):
    deployment_id = "deployment-id"
    from datarobot.models.data_drift import TargetDrift

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    mocker.patch.object(TargetDrift, "get", return_value=TargetDrift())

    operator = GetTargetDriftOperator(
        task_id="score_predictions", deployment_id=deployment_id
    )

    target_drift = operator.execute(context=dict())

    assert type(target_drift) == dr.models.data_drift.TargetDrift


def test_operator_get_feature_drift(mocker):
    deployment_id = "deployment-id"
    from datarobot.models.data_drift import FeatureDrift

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    mocker.patch.object(FeatureDrift, "list", return_value=[FeatureDrift()])

    operator = GetFeatureDriftOperator(
        task_id="score_predictions", deployment_id="deployment-id"
    )

    feature_drift = operator.execute(context=dict())

    assert type(feature_drift) == list
    assert all(type(fd) == dr.models.data_drift.FeatureDrift for fd in feature_drift)
