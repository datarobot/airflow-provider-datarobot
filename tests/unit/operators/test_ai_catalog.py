# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from unittest.mock import ANY

import datarobot as dr
import freezegun
import pytest

from datarobot_provider.operators.ai_catalog import CreateDatasetFromDataStoreOperator
from datarobot_provider.operators.ai_catalog import CreateDatasetFromProjectOperator
from datarobot_provider.operators.ai_catalog import CreateDatasetFromRecipeOperator
from datarobot_provider.operators.ai_catalog import CreateDatasetVersionOperator
from datarobot_provider.operators.ai_catalog import CreateOrUpdateDataSourceOperator
from datarobot_provider.operators.ai_catalog import CreateWranglingRecipeOperator
from datarobot_provider.operators.ai_catalog import UpdateDatasetFromFileOperator
from datarobot_provider.operators.ai_catalog import UploadDatasetOperator


def test_operator_upload_dataset(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    upload_dataset_mock = mocker.patch.object(dr.Dataset, "upload", return_value=dataset_mock)
    context = {"params": {"dataset_file_path": "/path/to/local/file"}}

    operator = UploadDatasetOperator(task_id="upload_dataset")
    operator.render_template_fields(context)
    dataset_id = operator.execute(context=context)

    assert dataset_id == "dataset-id"
    upload_dataset_mock.assert_called_with(source="/path/to/local/file")


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


def test_operator_create_wrangling_recipe_from_dataset(mocker):
    get_dataset_mock = mocker.patch.object(dr.Dataset, "get")
    get_exp_container_mock = mocker.patch.object(dr.UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.dr.models.Recipe")
    recipe_mock.from_dataset.return_value.id = "test-recipe-id"
    context = {"params": {"use_case_id": "test-use-case-id"}}

    operator = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        recipe_name="Test name",
        dataset_id="test-dataset-id",
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            }
        ],
        downsampling_directive=dr.enums.DownsamplingOperations.RANDOM_SAMPLE,
        downsampling_arguments={"value": 100, "seed": 25},
    )
    operator.render_template_fields(context)

    recipe_id = operator.execute(context=context)

    assert recipe_id == "test-recipe-id"
    get_dataset_mock.assert_called_once_with("test-dataset-id")
    get_exp_container_mock.assert_called_once_with("test-use-case-id")
    client_mock.patch.assert_called_once_with(
        "recipes/test-recipe-id/",
        json={"name": "Test name", "description": "Created with Apache-Airflow"},
    )
    recipe_mock.from_dataset.assert_called_once_with(
        get_exp_container_mock.return_value,
        get_dataset_mock.return_value,
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
    )
    recipe_mock.set_operations.assert_called_once()
    assert isinstance(
        recipe_mock.set_operations.call_args.args[1][0], dr.models.recipe.WranglingOperation
    )

    recipe_mock.update_downsampling.assert_called_once()
    assert (
        recipe_mock.update_downsampling.call_args.args[1].directive
        == dr.enums.DownsamplingOperations.RANDOM_SAMPLE
    )
    assert recipe_mock.update_downsampling.call_args.args[1].arguments == {"value": 100, "seed": 25}


@freezegun.freeze_time("2022-01-01 14:32:16")
def test_operator_create_wrangling_recipe_from_db_table(mocker):
    get_datastore_mock = mocker.patch.object(
        dr.DataStore, "get", return_value=dr.DataStore(data_store_type="jdbc")
    )
    get_exp_container_mock = mocker.patch.object(dr.UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.dr.models.Recipe")
    recipe_mock.from_data_store.return_value.id = "test-recipe-id"
    context = {
        "params": {
            "use_case_id": "test-use-case-id",
            "table_schema": "PUBLIC",
            "table_name": "TestTable",
        }
    }

    operator = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        recipe_name="Test name",
        data_store_id="test-data-store-id",
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            }
        ],
        downsampling_directive=dr.enums.DownsamplingOperations.RANDOM_SAMPLE,
        downsampling_arguments={"value": 100, "seed": 25},
    )
    operator.render_template_fields(context)

    recipe_id = operator.execute(context=context)

    assert recipe_id == "test-recipe-id"
    get_datastore_mock.assert_called_once_with("test-data-store-id")
    get_exp_container_mock.assert_called_once_with("test-use-case-id")
    client_mock.patch.assert_called_once_with(
        "recipes/test-recipe-id/",
        json={"name": "Test name", "description": "Created with Apache-Airflow"},
    )
    recipe_mock.from_data_store.assert_called_once_with(
        get_exp_container_mock.return_value,
        get_datastore_mock.return_value,
        data_source_type=dr.enums.DataWranglingDataSourceTypes.JDBC,
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        data_source_inputs=ANY,
    )
    assert (
        recipe_mock.from_data_store.call_args.kwargs["data_source_inputs"][0].canonical_name
        == "Airflow:PUBLIC-TestTable-2022-01-01T14:32:16"
    )
    assert recipe_mock.from_data_store.call_args.kwargs["data_source_inputs"][0].schema == "PUBLIC"
    assert (
        recipe_mock.from_data_store.call_args.kwargs["data_source_inputs"][0].table == "TestTable"
    )
    recipe_mock.set_operations.assert_called_once()
    recipe_mock.update_downsampling.assert_called_once()


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
    mocker, test_params, do_snapshot, expected_name, expected_mat_destination
):
    dataset_mock = mocker.Mock()
    recipe_mock = mocker.Mock(recipe_id="test-recipe-id")
    dataset_mock.id = "dataset-id"
    create_dataset_from_recipe_mock = mocker.patch.object(
        dr.Dataset, "create_from_recipe", return_value=dataset_mock
    )
    get_recipe_mock = mocker.patch.object(dr.models.Recipe, "get", return_value=recipe_mock)

    context = {"params": test_params}
    operator = CreateDatasetFromRecipeOperator(
        task_id="create_from_recipe",
        recipe_id="test-recipe-id",
        dataset_name_param="dataset1_name",
        do_snapshot=do_snapshot,
    )
    operator.render_template_fields(context)

    dataset_id = operator.execute(context=context)

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
