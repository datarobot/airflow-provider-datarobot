# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.data_registry import CreateDatasetFromDataStoreOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromProjectOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromRecipeOperator
from datarobot_provider.operators.data_registry import CreateDatasetVersionOperator
from datarobot_provider.operators.data_registry import CreateOrUpdateDataSourceOperator
from datarobot_provider.operators.data_registry import GetDataStoreOperator
from datarobot_provider.operators.data_registry import UpdateDatasetFromFileOperator
from datarobot_provider.operators.data_registry import UploadDatasetOperator


def test_operator_get_data_store(mocker):
    mocker.patch.object(
        dr.DataStore,
        "list",
        return_value=[
            dr.DataStore(data_store_id="0", canonical_name="the connection"),
            dr.DataStore(data_store_id="1", canonical_name="The connection"),
            dr.DataStore(data_store_id="2", canonical_name="The connection."),
        ],
    )

    operator = GetDataStoreOperator(task_id="test", data_connection="The connection")
    data_store_id = operator.execute({})

    assert data_store_id == "1"


def test_operator_upload_dataset(mocker, xcom_context):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    upload_dataset_mock = mocker.patch.object(
        dr.Dataset, "create_from_url", return_value=dataset_mock
    )
    xcom_context["params"] = {"dataset_file_path": "https://path.to/remote/file.csv"}

    operator = UploadDatasetOperator(task_id="upload_dataset")
    operator.render_template_fields(xcom_context)
    dataset_id = operator.execute(context=xcom_context)

    assert dataset_id == "dataset-id"
    upload_dataset_mock.assert_called_with(url="https://path.to/remote/file.csv", max_wait=3600)


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


def test_operator_create_dataset_from_jdbc(mocker):
    context = {
        "params": {
            "dataset_name": "test_dataset_name",
            "table_schema": "integration_demo",
            "table_name": "test_table",
            "query": 'SELECT * FROM "integration_demo"."test_table"',
            "persist_data_after_ingestion": True,
            "do_snapshot": True,
            "max_wait": 3600,
        }
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
    add_into_use_case_mock = mocker.patch.object(
        CreateDatasetFromDataStoreOperator, "add_into_use_case"
    )

    operator = CreateDatasetFromDataStoreOperator(
        task_id="load_jdbc_dataset", data_store_id="test", credential_id="test-cred-id"
    )
    dataset_id = operator.execute(context=context)

    assert dataset_id == "dataset-id"

    create_jdbc_dataset_mock.assert_called_with(
        data_source_id="datasource-id",
        credential_id="test-cred-id",
        persist_data_after_ingestion=True,
        do_snapshot=True,
        max_wait=3600,
    )
    add_into_use_case_mock.assert_called_once_with(dataset_mock, context=context)


@pytest.mark.parametrize(
    "test_params, do_snapshot, expected_name, expected_mat_destination",
    [
        ({}, True, None, None),
        (
            {"dataset1_name": "The dataset name.", "dataset_name": "another"},
            True,
            "The dataset name.",
            None,
        ),
        (
            {"materialization_schema": "testSchema", "materialization_table": "testTable"},
            False,
            "testTable",
            {"catalog": None, "schema": "testSchema", "table": "testTable"},
        ),
    ],
)
def test_operator_create_dataset_from_recipe(
    mocker, xcom_context, test_params, do_snapshot, expected_name, expected_mat_destination
):
    dataset_mock = mocker.Mock()
    recipe_mock = mocker.Mock(recipe_id="test-recipe-id")
    dataset_mock.id = "dataset-id"
    create_dataset_from_recipe_mock = mocker.patch.object(
        dr.Dataset, "create_from_recipe", return_value=dataset_mock
    )
    get_recipe_mock = mocker.patch.object(dr.models.Recipe, "get", return_value=recipe_mock)

    xcom_context["params"] = test_params
    operator = CreateDatasetFromRecipeOperator(
        task_id="create_from_recipe",
        recipe_id="test-recipe-id",
        dataset_name="{{ params.dataset1_name }}",
        do_snapshot=do_snapshot,
    )
    operator.render_template_fields(xcom_context)

    dataset_id = operator.execute(context=xcom_context)

    assert dataset_id == "dataset-id"
    create_dataset_from_recipe_mock.assert_called_once_with(
        recipe_mock,
        name=expected_name,
        do_snapshot=do_snapshot,
        persist_data_after_ingestion=True,
        materialization_destination=expected_mat_destination,
    )
    get_recipe_mock.assert_called_once_with("test-recipe-id")


def test_operator_create_dataset_from_project(mocker):
    dataset_mock = mocker.Mock(id="dataset-id")
    create_dataset_from_project_mock = mocker.patch.object(
        dr.Dataset, "create_from_project", return_value=dataset_mock
    )

    operator = CreateDatasetFromProjectOperator(
        task_id="create_project_from_project_id",
        project_id="project-id",
    )

    dataset_id = operator.execute(context={})

    assert dataset_id == "dataset-id"
    create_dataset_from_project_mock.assert_called_once_with(project_id="project-id")


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
    new_query = 'SELECT * FROM "integration_demo"."test_table22"'
    canonical_name = "Airflow-test-data-store-id-q-fcd91844f7cf2132d6a98e7692d0dae808f77d800880870963be31ec00ac8ba8"

    context = {"params": {"db_query": new_query}}

    datastore_mock = mocker.Mock()
    datastore_mock.id = data_store_id

    datasource_mock = mocker.Mock()
    datasource_mock.id = "test-datasource-id"
    datasource_mock.canonical_name = canonical_name
    datasource_mock.params = dr.DataSourceParameters(
        query='SELECT * FROM "integration_demo"."test_table"'
    )
    datastore_get_mock = mocker.patch.object(dr.DataStore, "get", return_value=datastore_mock)
    mocker.patch.object(dr.DataSource, "list", return_value=[datasource_mock])

    create_datasource_mock = mocker.patch.object(dr.DataSource, "create")

    operator = CreateOrUpdateDataSourceOperator(
        task_id="create_dataset_version", data_store_id=data_store_id, query="{{ params.db_query }}"
    )
    operator.render_template_fields(context)
    data_source_id = operator.execute(context)

    assert data_source_id == "test-datasource-id"
    datastore_get_mock.assert_called_with(data_store_id=data_store_id)

    datasource_mock.update.assert_called_once_with(
        canonical_name=canonical_name,
        params=dr.DataSourceParameters(data_store_id=data_store_id, query=new_query),
    )
    create_datasource_mock.assert_not_called()
