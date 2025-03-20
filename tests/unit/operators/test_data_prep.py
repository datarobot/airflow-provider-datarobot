from unittest.mock import ANY, Mock

import freezegun
import pytest
from datarobot import Dataset, UseCase, DataStore, RelationshipsConfiguration
from datarobot.enums import DataWranglingDialect, DownsamplingOperations, \
    DataWranglingDataSourceTypes, RecipeInputType
from datarobot.models import RecipeDatasetInput, JDBCTableDataSourceInput, Recipe
from datarobot.models.recipe_operation import WranglingOperation

from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator
from datarobot_provider.operators.data_prep import CreateWranglingRecipeOperator, \
    CreateFeatureDiscoveryRecipeOperator


def test_operator_create_wrangling_recipe_from_dataset(mocker):
    get_dataset_mock = mocker.patch.object(Dataset, "get")
    get_exp_container_mock = mocker.patch.object(UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.Recipe")
    recipe_mock.from_dataset.return_value.id = "test-recipe-id"
    context = {"params": {"use_case_id": "test-use-case-id"}}

    operator = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        recipe_name="Test name",
        dataset_id="test-dataset-id",
        dialect=DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            }
        ],
        downsampling_directive=DownsamplingOperations.RANDOM_SAMPLE,
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
        dialect=DataWranglingDialect.SNOWFLAKE,
    )
    recipe_mock.set_operations.assert_called_once()
    assert isinstance(recipe_mock.set_operations.call_args.args[1][0], WranglingOperation)

    recipe_mock.update_downsampling.assert_called_once()
    assert (
        recipe_mock.update_downsampling.call_args.args[1].directive
        == DownsamplingOperations.RANDOM_SAMPLE
    )
    assert recipe_mock.update_downsampling.call_args.args[1].arguments == {"value": 100, "seed": 25}


@freezegun.freeze_time("2022-01-01 14:32:16")
def test_operator_create_wrangling_recipe_from_db_table(mocker):
    get_datastore_mock = mocker.patch.object(
        DataStore, "get", return_value=DataStore(data_store_type="jdbc")
    )
    get_exp_container_mock = mocker.patch.object(UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.Recipe")
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
        dialect=DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            }
        ],
        downsampling_directive=DownsamplingOperations.RANDOM_SAMPLE,
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
        data_source_type=DataWranglingDataSourceTypes.JDBC,
        dialect=DataWranglingDialect.SNOWFLAKE,
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


def test_operator_create_wrangling_recipe_join_dataset(mocker):
    get_dataset_mock = mocker.patch.object(
        Dataset, "get", return_value=Mock(version_id="secondary-dataset-version-id")
    )
    get_exp_container_mock = mocker.patch.object(UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.Recipe")
    recipe_mock.from_dataset.return_value.id = "test-recipe-id"
    recipe_mock.from_dataset.return_value.inputs = [
        RecipeDatasetInput(
            input_type=RecipeInputType.DATASET,
            dataset_id="test-dataset-id",
            dataset_version_id="legacy-version-id",
        )
    ]
    context = {"params": {"use_case_id": "test-use-case-id"}}

    operator = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        recipe_name="Test name",
        dataset_id="test-dataset-id",
        dialect=DataWranglingDialect.SPARK,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            },
            {
                "directive": "join",
                "arguments": {
                    "leftKeys": ["key-left"],
                    "rightKeys": ["key-right"],
                    "joinType": "inner",
                    "source": "dataset",
                    "rightDatasetId": "secondary-dataset-id",
                },
            },
            {
                "directive": "join",
                "arguments": {
                    "leftKeys": ["key-left"],
                    "rightKeys": ["key-right"],
                    "joinType": "inner",
                    "source": "dataset",
                    "rightDatasetId": "test-dataset-id",
                },
            },
        ],
        downsampling_directive=DownsamplingOperations.RANDOM_SAMPLE,
        downsampling_arguments={"value": 100, "seed": 25},
    )
    operator.render_template_fields(context)

    recipe_id = operator.execute(context=context)

    assert recipe_id == "test-recipe-id"
    assert get_dataset_mock.call_count == 3
    get_exp_container_mock.assert_called_once_with("test-use-case-id")
    client_mock.patch.assert_called_once_with(
        "recipes/test-recipe-id/",
        json={"name": "Test name", "description": "Created with Apache-Airflow"},
    )
    recipe_mock.from_dataset.assert_called_once_with(
        get_exp_container_mock.return_value,
        get_dataset_mock.return_value,
        dialect=DataWranglingDialect.SPARK,
    )
    recipe_mock.set_operations.assert_called_once_with("test-recipe-id", ANY)
    assert len(recipe_mock.set_operations.call_args.args[1]) == 3

    recipe_mock.update_downsampling.assert_called_once()
    assert (
        recipe_mock.update_downsampling.call_args.args[1].directive
        == DownsamplingOperations.RANDOM_SAMPLE
    )
    assert recipe_mock.update_downsampling.call_args.args[1].arguments == {"value": 100, "seed": 25}
    recipe_mock.set_inputs.assert_called_once_with("test-recipe-id", ANY)
    assert len(recipe_mock.set_inputs.call_args.args[1]) == 2

    assert recipe_mock.set_inputs.call_args_list[0].args[1][0].dataset_id == "test-dataset-id"
    assert (
        recipe_mock.set_inputs.call_args_list[0].args[1][0].dataset_version_id
        == "legacy-version-id"
    )

    assert recipe_mock.set_inputs.call_args.args[1][1].dataset_id == "secondary-dataset-id"
    assert (
        recipe_mock.set_inputs.call_args.args[1][1].dataset_version_id
        == "secondary-dataset-version-id"
    )


def test_operator_create_wrangling_recipe_join_datasource(mocker):
    data_store_id = "test-data-store-id"
    get_datastore_mock = mocker.patch.object(
        DataStore, "get", return_value=DataStore(data_store_type="jdbc")
    )
    get_exp_container_mock = mocker.patch.object(UseCase, "get")
    client_mock = mocker.patch(
        "datarobot_provider.operators.ai_catalog.dr.client.get_client"
    ).return_value
    recipe_mock = mocker.patch("datarobot_provider.operators.ai_catalog.Recipe")
    recipe_mock.from_data_store.return_value.id = "test-recipe-id"
    recipe_mock.from_data_store.return_value.inputs = [
        JDBCTableDataSourceInput(
            data_store_id=data_store_id,
            data_source_id="primary_data_source_id",
            input_type=RecipeInputType.DATASOURCE,
        ),
    ]
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
        data_store_id=data_store_id,
        dialect=DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {"columns": ["test-feature-1", "test-feature-2"]},
            },
            {
                "directive": "join",
                "arguments": {
                    "leftKeys": ["key-left"],
                    "rightKeys": ["key-right"],
                    "joinType": "inner",
                    "source": "table",
                    "rightDataSourceId": "secondary-datasource-id-1",
                },
            },
            {
                "directive": "join",
                "arguments": {
                    "leftKeys": ["key-left"],
                    "rightKeys": ["key-right"],
                    "joinType": "inner",
                    "source": "table",
                    "rightDataSourceId": "secondary-datasource-id-2",
                },
            },
        ],
        downsampling_directive=DownsamplingOperations.RANDOM_SAMPLE,
        downsampling_arguments={"value": 100, "seed": 25},
    )
    operator.render_template_fields(context)

    recipe_id = operator.execute(context=context)

    assert recipe_id == "test-recipe-id"
    get_datastore_mock.assert_called_once_with(data_store_id)
    get_exp_container_mock.assert_called_once_with("test-use-case-id")
    client_mock.patch.assert_called_once_with(
        "recipes/test-recipe-id/",
        json={"name": "Test name", "description": "Created with Apache-Airflow"},
    )

    recipe_mock.set_operations.assert_called_once_with("test-recipe-id", ANY)
    recipe_mock.update_downsampling.assert_called_once()
    recipe_mock.set_inputs.assert_called_once_with("test-recipe-id", ANY)
    assert len(recipe_mock.set_inputs.call_args.args[1]) == 3
    assert {x.data_store_id for x in recipe_mock.set_inputs.call_args.args[1]} == {data_store_id}
    assert [x.data_source_id for x in recipe_mock.set_inputs.call_args.args[1]] == [
        "primary_data_source_id",
        "secondary-datasource-id-1",
        "secondary-datasource-id-2",
    ]


@pytest.fixture
def dataset_definitions():
    return [
        {
            "identifier": "profile",
            "catalogId": "64ea423b666704a28e8fd613",
            "catalogVersionId": "64ea423c666704a28e8fd614",
            "snapshotPolicy": "latest",
        },
        {
            "identifier": "transactions",
            "catalogId": "64ea42e1666704a28e8fd6ac",
            "catalogVersionId": "64ea42e1666704a28e8fd6ad",
            "snapshotPolicy": "latest",
            "primaryTemporalKey": "Date",
        },
    ]


@pytest.fixture
def relationships():
    return [
        {
            "dataset2Identifier": "profile",
            "dataset1Keys": ["CustomerID"],
            "dataset2Keys": ["CustomerID"],
            "featureDerivationWindows": [
                {"start": -7, "end": -1, "unit": "DAY"},
                {"start": -14, "end": -1, "unit": "DAY"},
                {"start": -30, "end": -1, "unit": "DAY"},
            ],
            "predictionPointRounding": 1,
            "predictionPointRoundingTimeUnit": "DAY",
        },
        {
            "dataset2Identifier": "transactions",
            "dataset1Keys": ["CustomerID"],
            "dataset2Keys": ["CustomerID"],
            "dataset1Identifier": "profile",
        },
    ]


@pytest.fixture
def feature_discovery_settings():
    return [
        {"name": "enable_days_from_prediction_point", "value": True},
        {"name": "enable_hour", "value": True},
        {"name": "enable_categorical_num_unique", "value": False},
        {"name": "enable_categorical_statistics", "value": False},
        {"name": "enable_numeric_minimum", "value": True},
        {"name": "enable_token_counts", "value": False},
        {"name": "enable_latest_value", "value": True},
        {"name": "enable_numeric_standard_deviation", "value": True},
        {"name": "enable_numeric_skewness", "value": False},
        {"name": "enable_day_of_week", "value": True},
        {"name": "enable_entropy", "value": False},
        {"name": "enable_numeric_median", "value": True},
        {"name": "enable_word_count", "value": False},
        {"name": "enable_pairwise_time_difference", "value": True},
        {"name": "enable_days_since_previous_event", "value": True},
        {"name": "enable_numeric_maximum", "value": True},
        {"name": "enable_numeric_kurtosis", "value": False},
        {"name": "enable_most_frequent", "value": False},
        {"name": "enable_day", "value": True},
        {"name": "enable_numeric_average", "value": True},
        {"name": "enable_summarized_counts", "value": False},
        {"name": "enable_missing_count", "value": True},
        {"name": "enable_record_count", "value": True},
        {"name": "enable_numeric_sum", "value": True},
    ]


@pytest.mark.parametrize("remove_version_id", [True, False])
def test_create_feature_discovery_recipe(
    mocker, dataset_definitions, relationships, feature_discovery_settings, remove_version_id
):
    recipe_settings_mock = mocker.Mock(relationships_configuration_id="recipe_config_id")
    recipe_mock = mocker.Mock(id="recipe_id", settings=recipe_settings_mock)
    create_recipe_mock = mocker.patch.object(Recipe, "from_dataset", return_value=recipe_mock)
    dataset_mock = mocker.Mock(version_id="version-id")
    mocker.patch.object(Dataset, "get", return_value=dataset_mock)
    use_case_mock = mocker.Mock()
    mocker.patch.object(BaseUseCaseEntityOperator, "get_use_case", return_value=use_case_mock)

    replace_config_mock = mocker.patch.object(RelationshipsConfiguration, "replace")

    if remove_version_id:
        # Test we auto-add version ID when absent in CreateFeatureDiscoveryRecipeOperator
        for d in dataset_definitions:
            d.pop("catalogVersionId")

    operator = CreateFeatureDiscoveryRecipeOperator(
        task_id="create_feature_discovery_recipe_operator",
        dataset_id="dataset_id",
        use_case_id="use_case_id",
        dataset_definitions=dataset_definitions,
        relationships=relationships,
        feature_discovery_settings=feature_discovery_settings,
    )

    operator_result = operator.execute(context={"params": {}})

    create_recipe_mock.assert_called_once_with(
        use_case_mock,
        dataset_mock,
        recipe_type="FEATURE_DISCOVERY",
    )

    expected_dataset_definitions = dataset_definitions
    if remove_version_id:
        for d in expected_dataset_definitions:
            d["catalogVersionId"] = "replace-version-id"

    replace_config_mock.assert_called_once_with(
        expected_dataset_definitions, relationships, feature_discovery_settings
    )

    assert operator_result == "recipe_id"