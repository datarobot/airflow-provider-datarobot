# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.ai_catalog import (
    CreateDatasetFromDataStoreOperator,
    CreateDatasetVersionOperator,
    CreateOrUpdateDataSourceOperator,
    UpdateDatasetFromFileOperator,
    UploadDatasetOperator,
)


def test_operator_upload_dataset(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    upload_dataset_mock = mocker.patch.object(
        dr.Dataset, "create_from_file", return_value=dataset_mock
    )

    operator = UploadDatasetOperator(task_id="upload_dataset")
    dataset_id = operator.execute(
        context={
            "params": {
                "dataset_file_path": "/path/to/local/file",
            },
        }
    )

    assert dataset_id == "dataset-id"
    upload_dataset_mock.assert_called_with(file_path="/path/to/local/file", max_wait=3600)


def test_operator_update_dataset_from_file(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    dataset_mock.version_id = "dataset-version-id"
    create_version_from_file_mock = mocker.patch.object(
        dr.Dataset, "create_version_from_file", return_value=dataset_mock
    )

    operator = UpdateDatasetFromFileOperator(task_id="create_version_dataset")
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
        dataset_id="dataset-id", file_path="/path/to/local/file", max_wait=3600
    )


def test_operator_create_dataset_from_jdbc(mocker, mock_airflow_connection_datarobot_jdbc):
    credential_data = {"credentialType": "basic", "user": "test_login", "password": "test_password"}
    test_params = {
        "datarobot_jdbc_connection": "datarobot_test_connection_jdbc_test",
        "dataset_name": "test_dataset_name",
        "table_schema": "integration_demo",
        "table_name": "test_table",
        "query": 'SELECT * FROM "integration_demo"."test_table"',
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
        "max_wait": 3600,
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

    operator = CreateDatasetFromDataStoreOperator(task_id="load_jdbc_dataset")
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
        max_wait=3600,
    )


def test_operator_create_dataset_version(mocker):
    dataset_id = "test-dataset-id"
    datasource_id = "test-datasource-id"
    credential_id = "test-credential-id"

    dataset_version_mock = mocker.Mock()
    dataset_version_mock.version_id = "test-dataset-version-id"
    create_dataset_version_mock = mocker.patch.object(
        dr.Dataset, "create_version_from_data_source", return_value=dataset_version_mock
    )

    operator = CreateDatasetVersionOperator(
        task_id="create_dataset_version",
        dataset_id=dataset_id,
        datasource_id=datasource_id,
        credential_id=credential_id,
    )
    dataset_version_id = operator.execute(context={})

    assert dataset_version_id == "test-dataset-version-id"
    create_dataset_version_mock.assert_called_with(
        dataset_id=dataset_id,
        data_source_id=datasource_id,
        credential_id=credential_id,
        max_wait=3600,
    )


def test_operator_create_datasource_operator(mocker):
    data_store_id = "test-data-store-id"

    test_params = {
        "datarobot_jdbc_connection": "datarobot_test_connection_jdbc_test",
        "dataset_name": "test_dataset_name",
        "table_schema": "integration_demo",
        "table_name": "test_table",
        "query": 'SELECT * FROM "integration_demo"."test_table"',
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
        "max_wait": 3600,
    }

    datastore_mock = mocker.Mock()
    datastore_mock.version_id = data_store_id

    datasource_params_mock = dr.DataSourceParameters(
        query='SELECT * FROM "integration_demo"."test_new_table"'
    )

    other_datasource_mock = mocker.Mock()
    other_datasource_mock.id = "test-other-datasource-id"
    other_datasource_mock.canonical_name = "other-datasource-name"
    other_datasource_mock.params = datasource_params_mock
    mocker.patch.object(dr.DataSource, "list", return_value=[other_datasource_mock])

    datasource_mock = mocker.Mock()
    datasource_mock.id = "test-datasource-id"
    datasource_mock.canonical_name = test_params["dataset_name"]
    datasource_mock.params = datasource_params_mock
    datastore_get_mock = mocker.patch.object(dr.DataStore, "get", return_value=datastore_mock)
    mocker.patch.object(dr.DataSource, "create", return_value=datasource_mock)

    datasource_update_mock = mocker.patch.object(datasource_mock, "update")

    create_dataset_mock = mocker.patch.object(dr.DataSource, "create", return_value=datasource_mock)

    operator = CreateOrUpdateDataSourceOperator(
        task_id="create_dataset_version",
        data_store_id=data_store_id,
    )
    data_source_id = operator.execute(
        context={
            "params": test_params,
        }
    )

    assert data_source_id == "test-datasource-id"
    datastore_get_mock.assert_called_with(data_store_id=data_store_id)

    datasource_update_mock.assert_not_called()
    create_dataset_mock.assert_called()


def test_operator_update_datasource_operator(mocker):
    data_store_id = "test-data-store-id"

    test_params = {
        "datarobot_jdbc_connection": "datarobot_test_connection_jdbc_test",
        "dataset_name": "test_dataset_name",
        "table_schema": "integration_demo",
        "table_name": "test_table",
        "query": 'SELECT * FROM "integration_demo"."test_table22"',
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
        "max_wait": 3600,
    }

    datastore_mock = mocker.Mock()
    datastore_mock.version_id = data_store_id

    datasource_mock = mocker.Mock()
    datasource_mock.id = "test-datasource-id"
    datasource_mock.canonical_name = test_params["dataset_name"]
    datasource_mock.params = dr.DataSourceParameters(
        query='SELECT * FROM "integration_demo"."test_table"'
    )
    datastore_get_mock = mocker.patch.object(dr.DataStore, "get", return_value=datastore_mock)
    mocker.patch.object(dr.DataSource, "list", return_value=[datasource_mock])
    mocker.patch.object(dr.DataSource, "create", return_value=datasource_mock)

    datasource_update_mock = mocker.patch.object(datasource_mock, "update")

    create_dataset_mock = mocker.patch.object(dr.DataSource, "create", return_value=datasource_mock)

    operator = CreateOrUpdateDataSourceOperator(
        task_id="create_dataset_version",
        data_store_id=data_store_id,
    )
    data_source_id = operator.execute(
        context={
            "params": test_params,
        }
    )

    assert data_source_id == "test-datasource-id"
    datastore_get_mock.assert_called_with(data_store_id=data_store_id)

    datasource_update_mock.assert_called()
    create_dataset_mock.assert_not_called()
