# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.ai_catalog import CreateDatasetFromJDBCOperator
from datarobot_provider.operators.ai_catalog import UpdateDatasetFromFileOperator
from datarobot_provider.operators.ai_catalog import UploadDatasetOperator


def test_operator_upload_dataset(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    upload_dataset_mock = mocker.patch.object(
        dr.Dataset, "create_from_file", return_value=dataset_mock
    )

    operator = UploadDatasetOperator(task_id='upload_dataset')
    dataset_id = operator.execute(
        context={
            "params": {
                "dataset_file_path": "/path/to/local/file",
            },
        }
    )

    assert dataset_id == "dataset-id"
    upload_dataset_mock.assert_called_with("/path/to/local/file")


def test_operator_update_dataset_from_file(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    dataset_mock.version_id = "dataset-version-id"
    create_version_from_file_mock = mocker.patch.object(
        dr.Dataset, "create_version_from_file", return_value=dataset_mock
    )

    operator = UpdateDatasetFromFileOperator(task_id='create_version_dataset')
    dataset_version_id = operator.execute(
        context={
            "params": {
                "training_dataset_id": "dataset-id",
                "dataset_file_path": "/path/to/local/file",
            },
        }
    )

    assert dataset_version_id == "dataset-version-id"
    create_version_from_file_mock.assert_called_with(
        dataset_id="dataset-id", file_path="/path/to/local/file"
    )


def test_operator_create_dataset_from_jdbc(mocker):
    credential_data = {"credentialType": "basic", "user": "test_login", "password": "test_password"}
    test_params = {
        "datarobot_jdbc_connection": "datarobot_jdbc_default",
        "dataset_name": "test_dataset_name",
        "table_schema": "integration_demo",
        "table_name": "test_table",
        "query": 'SELECT * FROM "integration_demo"."test_table"',
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
    }

    datasource_params_mock = mocker.Mock(
        dataset_name="test_dataset_name",
        table_schema="integration_demo",
        table_name="test_table",
        query='SELECT * FROM "integration_demo"."test_table"',
    )

    datasource_mock = mocker.Mock()
    datasource_mock.id = "datasource-id"
    datasource_mock.params = datasource_params_mock
    mocker.patch.object(dr.DataSource, "list", return_value=[])
    mocker.patch.object(dr.DataSource, "create", return_value=datasource_mock)

    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    create_jdbc_dataset_mock = mocker.patch.object(
        dr.Dataset, "create_from_data_source", return_value=dataset_mock
    )

    operator = CreateDatasetFromJDBCOperator(task_id='load_jdbc_dataset')
    dataset_id = operator.execute(
        context={
            "params": test_params,
        }
    )

    assert dataset_id == "dataset-id"

    create_jdbc_dataset_mock.assert_called_with(
        data_source_id="datasource-id",
        credential_data=credential_data,
        persist_data_after_ingestion=test_params["persist_data_after_ingestion"],
        do_snapshot=test_params["do_snapshot"],
    )
