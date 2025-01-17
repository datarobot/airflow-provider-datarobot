# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from datarobot import AUTOPILOT_MODE

from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.feature_discovery import DatasetDefinitionOperator
from datarobot_provider.operators.feature_discovery import DatasetRelationshipOperator
from datarobot_provider.operators.feature_discovery import RelationshipsConfigurationOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example", "feature discovery"],
    params={
        "project_name": "Demo Airflow DataRobot Feature Discovery",
        "autopilot_settings": {
            "target": "BadLoan",
            "mode": AUTOPILOT_MODE.QUICK,
            "feature_engineering_prediction_point": "date",
        },
        "advanced_options": {"feature_discovery_supervised_feature_reduction": True},
        "feature_discovery_settings": [
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
        ],
        "unsupervised_mode": False,
        "use_feature_discovery": True,
    },
)
def datarobot_feature_discovery_pipeline(
    training_dataset_id="64ea43b4465047fedb5ab3b2",
    training_dataset_version_id="64ea43b5465047fedb5ab3b3",
    profile_dataset_identifier="profile",
    profile_dataset_id="64ea423b666704a28e8fd613",
    profile_dataset_version_id="64ea423c666704a28e8fd614",
    transaction_dataset_identifier="transactions",
    transaction_dataset_id="64ea42e1666704a28e8fd6ac",
    transaction_dataset_version_id="64ea42e1666704a28e8fd6ad",
):
    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=training_dataset_id,
        dataset_version_id=training_dataset_version_id,
    )

    profile_dataset_definition_op = DatasetDefinitionOperator(
        task_id="profile_dataset_definition",
        dataset_identifier=profile_dataset_identifier,
        dataset_id=profile_dataset_id,
        dataset_version_id=profile_dataset_version_id,
    )

    transaction_dataset_definition_op = DatasetDefinitionOperator(
        task_id="transaction_dataset_definition",
        dataset_identifier=transaction_dataset_identifier,
        dataset_id=transaction_dataset_id,
        dataset_version_id=transaction_dataset_version_id,
        primary_temporal_key="Date",
    )

    # You do not need to specify dataset1Identifier when joining with the primary dataset
    primary_profile_relationship_op = DatasetRelationshipOperator(
        task_id="primary_profile_relationship",
        dataset2_identifier=profile_dataset_identifier,  # to profile
        dataset1_keys=["CustomerID"],  # on CustomerID
        dataset2_keys=["CustomerID"],
        feature_derivation_windows=[
            {"start": -7, "end": -1, "unit": "DAY"},
            {"start": -14, "end": -1, "unit": "DAY"},
            {"start": -30, "end": -1, "unit": "DAY"},
        ],  # example of multiple FDW
        prediction_point_rounding=1,
        prediction_point_rounding_time_unit="DAY",
    )

    relationship_definition_op = DatasetRelationshipOperator(
        task_id="relationship_definition",
        dataset1_identifier=profile_dataset_identifier,  # join profile
        dataset2_identifier=transaction_dataset_identifier,  # to transactions
        dataset1_keys=["CustomerID"],  # on CustomerID
        dataset2_keys=["CustomerID"],
    )

    relationships_configuration_op = RelationshipsConfigurationOperator(
        task_id="relationships_configuration",
        dataset_definitions=[
            profile_dataset_definition_op.output,
            transaction_dataset_definition_op.output,
        ],
        relationships=[primary_profile_relationship_op.output, relationship_definition_op.output],
    )

    train_models_op = StartAutopilotOperator(
        task_id="train_models",
        project_id=create_project_op.output,
        relationships_configuration_id=relationships_configuration_op.output,
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project_op.output,
    )

    (
        (profile_dataset_definition_op, transaction_dataset_definition_op)
        >> create_project_op
        >> (primary_profile_relationship_op, relationships_configuration_op)
        >> train_models_op
        >> autopilot_complete_sensor
    )


datarobot_feature_discovery_pipeline_dag = datarobot_feature_discovery_pipeline()

if __name__ == "__main__":
    datarobot_feature_discovery_pipeline_dag.test()
