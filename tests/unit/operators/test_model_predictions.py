# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.model_predictions import AddExternalDatasetOperator
from datarobot_provider.operators.model_predictions import RequestModelPredictionsOperator


def test_operator_add_external_dataset(mocker):
    project_id = "test-project-id"
    dataset_id = "dataset-id"
    external_dataset_id = "external_dataset_id"
    credential_id = "credential-id"
    dataset_version_id = "dataset-version-id"

    project_mock = mocker.Mock()
    project_mock.id = project_id
    project_mock.upload_dataset_from_catalog(project_id, dataset_id)

    get_project_mock = mocker.patch.object(dr.Project, "get", return_value=project_mock)
    external_dataset_mock = mocker.Mock()
    external_dataset_mock.id = external_dataset_id

    upload_dataset_from_catalog_mock = mocker.patch.object(
        project_mock, "upload_dataset_from_catalog", return_value=external_dataset_mock
    )

    operator = AddExternalDatasetOperator(
        task_id="add_external_dataset",
        project_id=project_id,
        dataset_id=dataset_id,
        credential_id=credential_id,
        dataset_version_id=dataset_version_id,
    )

    result = operator.execute(context={"params": {}})

    get_project_mock.assert_called_with(project_id)
    upload_dataset_from_catalog_mock.assert_called_with(
        dataset_id=dataset_id,
        credential_id=credential_id,
        dataset_version_id=dataset_version_id,
        max_wait=600,
    )
    assert result == external_dataset_id


def test_operator_request_model_predictions(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    external_dataset_id = "test-external-dataset-id"
    predictions_job_id = "test-predictions-job-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    get_model_mock = mocker.patch.object(dr.models.Model, "get", return_value=model_mock)

    predictions_job_mock = mocker.Mock()
    predictions_job_mock.id = predictions_job_id

    request_predictions_mock = mocker.patch.object(
        model_mock, "request_predictions", return_value=predictions_job_mock
    )

    operator = RequestModelPredictionsOperator(
        task_id="request_model_predictions",
        project_id=project_id,
        model_id=model_id,
        external_dataset_id=external_dataset_id,
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    request_predictions_mock.assert_called_with(dataset_id=external_dataset_id)
    assert result == predictions_job_id
