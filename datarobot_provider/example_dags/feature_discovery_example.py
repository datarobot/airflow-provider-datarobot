# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

import datarobot as dr

from airflow.decorators import dag
from datarobot import AUTOPILOT_MODE

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import CreateUseCaseOperator
from datarobot_provider.operators.feature_discovery import CreateFeatureDiscoveryRecipeOperator
from datarobot_provider.operators.feature_discovery import DatasetDefinitionOperator
from datarobot_provider.operators.feature_discovery import DatasetRelationshipOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    tags=["example", "csv", "feature discovery"],
    params={
        "project_name": "Demo Lending Club DataRobot Feature Discovery",
        "autopilot_settings": {
            "target": "BadLoan",
            "mode": AUTOPILOT_MODE.QUICK,
            "feature_engineering_prediction_point": "date",
        },
        "advanced_options": {"feature_discovery_supervised_feature_reduction": True},
    },
)
def datarobot_feature_discovery_pipeline(
    primary_dataset_path=(
        "https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/LendingClub/target.csv"
    ),
    profile_dataset_path=(
        "https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/LendingClub/profile.csv"
    ),
    transactions_dataset_path=(
        "https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/LendingClub/transactions.csv"
    ),
):
    # Create a Use Case to keep all subsequent assets. Default name is "Airflow"
    create_use_case = CreateUseCaseOperator(task_id="create_use_case")

    # Upload all the datasets into Data Registry.
    upload_primary_dataset = UploadDatasetOperator(
        task_id="upload_primary_dataset",
        file_path=primary_dataset_path,
        use_case_id=create_use_case.output,
    )
    upload_profile_dataset = UploadDatasetOperator(
        task_id="upload_profile_dataset",
        file_path=profile_dataset_path,
        use_case_id=create_use_case.output,
    )
    upload_transactions_dataset = UploadDatasetOperator(
        task_id="upload_transactions_dataset",
        file_path=transactions_dataset_path,
        use_case_id=create_use_case.output,
    )

    # TEMPORARY create client for debugging:
    dr.Client('<token>', 'https://staging.datarobot.com/api/v2')

    # Get the versions IDs of the secondary datasets created.
    profile_dataset_version_id = dr.Dataset.get(upload_profile_dataset.output).version_id
    transaction_dataset_version_id = dr.Dataset.get(upload_transactions_dataset.output).version_id

    # Define the secondary datasets for Feature Discovery
    profile_dataset_definition = DatasetDefinitionOperator(
        task_id="profile_dataset_definition",
        dataset_identifier="profile",
        dataset_id=upload_profile_dataset.output,
        dataset_version_id=profile_dataset_version_id,
    )

    transaction_dataset_definition = DatasetDefinitionOperator(
        task_id="transaction_dataset_definition",
        dataset_identifier="transactions",
        dataset_id=upload_transactions_dataset.output,
        dataset_version_id=transaction_dataset_version_id,
        primary_temporal_key="Date",
    )

    # Define the relationships between the datasets.
    # You do not need to specify dataset1Identifier when joining with the primary dataset.
    primary_profile_relationship = DatasetRelationshipOperator(
        task_id="primary_profile_relationship",
        dataset2_identifier="profile",  # to profile
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

    profile_transaction_relationship_definition = DatasetRelationshipOperator(
        task_id="relationship_definition",
        dataset1_identifier="profile",  # join profile
        dataset2_identifier="transactions",  # to transactions
        dataset1_keys=["CustomerID"],  # on CustomerID
        dataset2_keys=["CustomerID"],
    )

    dataset_definitions = [
        profile_dataset_definition.output, transaction_dataset_definition.output
    ]
    relationships = [
        primary_profile_relationship.output,
        profile_transaction_relationship_definition.output,
    ]

    feature_discovery_settings = [
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

    # Create a Feature Discovery recipe with all the above information.
    create_feature_discovery_recipe = CreateFeatureDiscoveryRecipeOperator(
        dataset_id=upload_primary_dataset.output,
        use_case_id=create_use_case.output,
        dataset_definitions=dataset_definitions,
        relationships=relationships,
        feature_discovery_settings=feature_discovery_settings,
    )

    # Create and launch a project from the recipe.
    create_project = CreateProjectOperator(
        recipe_id=create_feature_discovery_recipe.output, use_case_id=create_use_case.output
    )

    train_models = StartAutopilotOperator(task_id="train_models", project_id=create_project.output)

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete", project_id=create_project.output
    )

    (
        create_use_case
        >> (upload_primary_dataset, upload_profile_dataset, upload_transactions_dataset)
        >> (profile_dataset_definition, transaction_dataset_definition)
        >> (primary_profile_relationship, profile_transaction_relationship_definition)
        >> create_feature_discovery_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
    )


datarobot_feature_discovery_pipeline_dag = datarobot_feature_discovery_pipeline()

if __name__ == "__main__":
    datarobot_feature_discovery_pipeline_dag.test()
