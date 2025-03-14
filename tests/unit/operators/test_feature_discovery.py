# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator
from datarobot_provider.operators.feature_discovery import CreateFeatureDiscoveryRecipeOperator


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
    create_recipe_mock = mocker.patch.object(
        dr.models.Recipe, "from_dataset", return_value=recipe_mock
    )
    dataset_mock = mocker.Mock(version_id="version-id")
    mocker.patch.object(dr.Dataset, "get", return_value=dataset_mock)
    use_case_mock = mocker.Mock()
    mocker.patch.object(BaseUseCaseEntityOperator, "get_use_case", return_value=use_case_mock)

    replace_config_mock = mocker.patch.object(dr.RelationshipsConfiguration, "replace")

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
