# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.model_predictions import AddExternalDatasetOperator


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
