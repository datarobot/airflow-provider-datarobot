# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

import datarobot as dr
import pytest
from datarobot.models.data_drift import FeatureDrift
from datarobot.models.data_drift import TargetDrift

from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import DeployModelOperator
from datarobot_provider.operators.datarobot import DeployRecommendedModelOperator
from datarobot_provider.operators.datarobot import GetFeatureDriftOperator
from datarobot_provider.operators.datarobot import GetTargetDriftOperator
from datarobot_provider.operators.datarobot import ScorePredictionsOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.operators.datarobot import _serialize_drift


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


def test_operator_create_project_from_dataset(mocker):
    project_mock = mocker.Mock()
    project_mock.id = "project-id"
    create_project_mock = mocker.patch.object(
        dr.Project, "create_from_dataset", return_value=project_mock
    )

    operator = CreateProjectOperator(task_id='create_project_from_dataset')
    project_id = operator.execute(
        context={
            "params": {
                "training_dataset_id": "some_dataset_id",
                "project_name": "test project",
                "unsupervised_mode": False,
                "use_feature_discovery": False,
            },
        }
    )

    assert project_id == "project-id"
    create_project_mock.assert_called_with(
        dataset_id='some_dataset_id', project_name='test project'
    )


def test_operator_train_models(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, "get", return_value=project_mock)

    operator = TrainModelsOperator(task_id="train_models", project_id="project-id")
    settings = {"target": "readmitted"}
    operator.execute(context={"params": {"autopilot_settings": settings}})

    project_mock.set_target.assert_called_with(**settings)


def test_operator_deploy_model(mocker):
    pred_server_mock = mocker.Mock()
    pred_server_mock.id = "pred-server-id"
    mocker.patch.object(dr.PredictionServer, "list", return_value=[pred_server_mock])
    deployment_mock = mocker.Mock()
    deployment_mock.id = "deployment-id"
    create_mock = mocker.patch.object(
        dr.Deployment, "create_from_learning_model", return_value=deployment_mock
    )

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
    create_mock = mocker.patch.object(
        dr.Deployment, "create_from_learning_model", return_value=deployment_mock
    )

    operator = DeployRecommendedModelOperator(
        task_id="deploy_recommended_model", project_id="project-id"
    )
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

    operator = ScorePredictionsOperator(task_id="score_predictions", deployment_id=deployment_id)

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
    operator = ScorePredictionsOperator(task_id="score_predictions", deployment_id="deployment-id")

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


@pytest.fixture
def drift_details():
    return {
        "period": {
            "start": datetime.fromisoformat("2000-01-01"),
            "end": datetime.fromisoformat("2000-01-07"),
        },
        "drift_score": 0.9,
    }


def test_operator_get_target_drift(mocker, drift_details):
    deployment_id = "deployment-id"

    target_drift = TargetDrift(**drift_details)
    expected_target_drift = _serialize_drift(TargetDrift(**drift_details))

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_drift_mock = mocker.patch.object(
        dr.Deployment, "get_target_drift", return_value=target_drift
    )

    operator = GetTargetDriftOperator(task_id="score_predictions", deployment_id=deployment_id)
    target_drift_params = {"target_drift": {"model_id": "5e29a5a65a5fe66a9ce399ae"}}

    drift = operator.execute(context={"params": target_drift_params})

    assert drift == expected_target_drift
    get_drift_mock.assert_called_with(**target_drift_params["target_drift"])


def test_operator_get_feature_drift(mocker, drift_details):
    deployment_id = "deployment-id"

    feature_drift = [FeatureDrift(**drift_details), FeatureDrift(**drift_details)]
    expected_feature_drift = [_serialize_drift(drift) for drift in feature_drift]

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_drift_mock = mocker.patch.object(
        dr.Deployment, "get_feature_drift", return_value=feature_drift
    )

    operator = GetFeatureDriftOperator(task_id="score_predictions", deployment_id="deployment-id")
    feature_drift_params = {"feature_drift": {"model_id": "5e29a5a65a5fe66a9ce399ae"}}

    drift = operator.execute(context={"params": feature_drift_params})

    assert drift == expected_feature_drift
    get_drift_mock.assert_called_with(**feature_drift_params["feature_drift"])
