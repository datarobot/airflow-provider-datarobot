# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import os

import datarobot as dr
import pytest

from datarobot_provider.operators.scoring_code import (
    DownloadDeploymentScoringCodeOperator,
    DownloadModelScoringCodeOperator,
)


@pytest.fixture
def scoring_code_download_params():
    return {
        "source_code": False,
        "include_agent": False,
        "include_prediction_explanations": False,
        "include_prediction_intervals": False,
    }


def test_operator_download_deployment_scoring_code(
    mocker, monkeypatch, scoring_code_download_params
):
    deployment_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = "/home/airflow/gcs/data/"
    expected_scoring_code_path = os.path.join(
        scoring_code_filepath, f"{deployment_id}-{model_id}.jar"
    )

    mocker.patch.object(
        dr.Deployment, "get", return_value=dr.Deployment(id=deployment_id, model={"id": model_id})
    )

    monkeypatch.setattr(os.path, "exists", lambda path: True)

    download_scoring_code_from_deployment_mock = mocker.patch.object(
        dr.Deployment, "download_scoring_code"
    )

    operator = DownloadDeploymentScoringCodeOperator(
        task_id="download_scoring_code_from_deployment",
        deployment_id="deployment-id",
        base_path=scoring_code_filepath,
    )

    operator_result = operator.execute(context={"params": scoring_code_download_params})
    assert operator_result == expected_scoring_code_path
    scoring_code_download_params["filepath"] = expected_scoring_code_path
    download_scoring_code_from_deployment_mock.assert_called_with(**scoring_code_download_params)


def test_operator_download_deployment_scoring_code_invalid_params(
    mocker, monkeypatch, scoring_code_download_params
):
    deployment_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = None

    mocker.patch.object(
        dr.Deployment, "get", return_value=dr.Deployment(id=deployment_id, model={"id": model_id})
    )

    mocker.patch.object(dr.Deployment, "download_scoring_code")

    operator = DownloadDeploymentScoringCodeOperator(
        task_id="download_scoring_code_from_deployment",
        deployment_id="deployment-id",
        base_path=scoring_code_filepath,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": scoring_code_download_params})


def test_operator_download_deployment_scoring_code_invalid_path(
    mocker, monkeypatch, scoring_code_download_params
):
    deployment_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = "/home/airflow/gcs/data/"

    mocker.patch.object(
        dr.Deployment, "get", return_value=dr.Deployment(id=deployment_id, model={"id": model_id})
    )

    monkeypatch.setattr(os.path, "exists", lambda path: False)

    operator = DownloadDeploymentScoringCodeOperator(
        task_id="download_scoring_code_from_deployment",
        deployment_id="deployment-id",
        base_path=scoring_code_filepath,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": scoring_code_download_params})


def test_operator_download_model_scoring_code(mocker, monkeypatch, scoring_code_download_params):
    project_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = "/home/airflow/gcs/data/"
    expected_scoring_code_path = os.path.join(scoring_code_filepath, f"{model_id}.jar")

    mocker.patch.object(dr.Model, "get", return_value=dr.Model(id=model_id, project_id=project_id))

    monkeypatch.setattr(os.path, "exists", lambda path: True)

    download_scoring_code_from_deployment_mock = mocker.patch.object(
        dr.Model, "download_scoring_code"
    )

    operator = DownloadModelScoringCodeOperator(
        task_id="download_scoring_code_from_model",
        project_id=project_id,
        model_id=model_id,
        base_path=scoring_code_filepath,
    )

    operator_result = operator.execute(context={"params": scoring_code_download_params})
    assert operator_result == expected_scoring_code_path
    params = {
        "file_name": expected_scoring_code_path,
        "source_code": scoring_code_download_params["source_code"],
    }
    download_scoring_code_from_deployment_mock.assert_called_with(**params)


def test_operator_download_model_scoring_code_none_path(
    mocker, monkeypatch, scoring_code_download_params
):
    project_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = None

    mocker.patch.object(dr.Model, "get", return_value=dr.Model(id=model_id, project_id=project_id))

    mocker.patch.object(dr.Model, "download_scoring_code")

    operator = DownloadModelScoringCodeOperator(
        task_id="download_scoring_code_from_model",
        project_id=project_id,
        model_id=model_id,
        base_path=scoring_code_filepath,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": scoring_code_download_params})


def test_operator_download_model_scoring_code_invalid_path(
    mocker, monkeypatch, scoring_code_download_params
):
    project_id = "deployment-id"
    model_id = "model-id"
    scoring_code_filepath = "/home/airflow/gcs/data/"

    mocker.patch.object(dr.Model, "get", return_value=dr.Model(id=model_id, project_id=project_id))

    monkeypatch.setattr(os.path, "exists", lambda path: False)

    mocker.patch.object(dr.Model, "download_scoring_code")

    operator = DownloadModelScoringCodeOperator(
        task_id="download_scoring_code_from_model",
        project_id=project_id,
        model_id=model_id,
        base_path=scoring_code_filepath,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": scoring_code_download_params})
