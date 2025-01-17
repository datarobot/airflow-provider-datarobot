# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.feature_discovery import (
    DatasetDefinitionOperator,
    DatasetRelationshipOperator,
    RelationshipsConfigurationOperator,
)


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


def test_operator_relationships_configuration(
    mocker, dataset_definitions, relationships, feature_discovery_settings
):
    relationships_configuration_mock = mocker.Mock(target=None)
    relationships_configuration_mock.id = "test-relationships-configuration-id"
    relationships_configuration_create_mock = mocker.patch.object(
        dr.RelationshipsConfiguration, "create", return_value=relationships_configuration_mock
    )

    operator = RelationshipsConfigurationOperator(
        task_id="test_relationships_configuration",
        dataset_definitions=dataset_definitions,
        relationships=relationships,
    )

    relationships_configuration_result = operator.execute(
        context={"params": {"feature_discovery_settings": feature_discovery_settings}}
    )

    relationships_configuration_create_mock.assert_called_with(
        dataset_definitions=dataset_definitions,
        relationships=relationships,
        feature_discovery_settings=feature_discovery_settings,
    )

    assert relationships_configuration_result == relationships_configuration_mock.id


def test_dataset_definition_operator(dataset_definitions):
    operator = DatasetDefinitionOperator(
        task_id="test_dataset_definition_operator",
        dataset_identifier=dataset_definitions[1]["identifier"],
        dataset_id=dataset_definitions[1]["catalogId"],
        dataset_version_id=dataset_definitions[1]["catalogVersionId"],
        primary_temporal_key=dataset_definitions[1]["primaryTemporalKey"],
    )

    operator_result = operator.execute(context={"params": {}})

    assert operator_result == dataset_definitions[1]


def test_dataset_relationship_operator(relationships):
    operator = DatasetRelationshipOperator(
        task_id="primary_profile_relationship",
        dataset2_identifier="profile",
        dataset1_keys=["CustomerID"],
        dataset2_keys=["CustomerID"],
        feature_derivation_windows=relationships[0][
            "featureDerivationWindows"
        ],  # example of multiple FDW
        prediction_point_rounding=1,
        prediction_point_rounding_time_unit="DAY",
    )

    operator_result = operator.execute(context={"params": {}})

    assert operator_result == relationships[0]
