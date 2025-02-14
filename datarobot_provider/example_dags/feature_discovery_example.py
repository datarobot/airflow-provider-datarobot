# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from airflow.decorators import dag
from airflow.models.baseoperator import cross_downstream
from datarobot import AUTOPILOT_MODE

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import GetOrCreateUseCaseOperator
from datarobot_provider.operators.feature_discovery import CreateFeatureDiscoveryRecipeOperator
from datarobot_provider.operators.feature_discovery import DatasetRelationshipOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    tags=["example", "csv", "feature discovery"],
    params={
        "autopilot_settings": {
            "target": "BadLoan",
            "mode": AUTOPILOT_MODE.QUICK,
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
    create_use_case = GetOrCreateUseCaseOperator(task_id="create_use_case")

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

    # Define the secondary datasets for Feature Discovery
    profile_dataset_definition = {
        "identifier": "profile",
        "catalogId": upload_profile_dataset.output,
    }

    transaction_dataset_definition = {
        "identifier": "transactions",
        "catalogId": upload_transactions_dataset.output,
    }

    # Alternatively, you can define a DatasetDefinitionOperator:
    # transaction_dataset_definition = DatasetDefinitionOperator(
    #     task_id="transaction_dataset_definition",
    #     dataset_identifier="transactions",
    #     dataset_id="<Dataset  ID>",
    #     dataset_version_id="<Dataset Version ID"<Dataset Version ID>">",
    # ).output

    # Define the relationships between the datasets.
    # You do not need to specify dataset1Identifier when joining with the primary dataset.
    primary_profile_relationship = DatasetRelationshipOperator(
        task_id="primary_profile_relationship",
        dataset2_identifier="profile",  # to profile
        dataset1_keys=["CustomerID"],  # on CustomerID
        dataset2_keys=["CustomerID"],
    )

    profile_transaction_relationship = DatasetRelationshipOperator(
        task_id="profile_transaction_relationship",
        dataset1_identifier="profile",  # join profile
        dataset2_identifier="transactions",  # to transactions
        dataset1_keys=["CustomerID"],  # on CustomerID
        dataset2_keys=["CustomerID"],
    )

    dataset_definitions = [profile_dataset_definition, transaction_dataset_definition]
    relationships = [
        primary_profile_relationship.output,
        profile_transaction_relationship.output,
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
        task_id="create_feature_discovery_recipe",
    )

    # Create and launch a project from the recipe.
    create_project = CreateProjectOperator(
        recipe_id=create_feature_discovery_recipe.output,
        use_case_id=create_use_case.output,
        task_id="create_project",
    )

    train_models = StartAutopilotOperator(task_id="train_models", project_id=create_project.output)

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete", project_id=create_project.output
    )

    (
        create_use_case
        >> [upload_primary_dataset, upload_profile_dataset, upload_transactions_dataset]
    )
    cross_downstream(
        [upload_primary_dataset, upload_profile_dataset, upload_transactions_dataset],
        [primary_profile_relationship, profile_transaction_relationship],
    )
    (
        [primary_profile_relationship, profile_transaction_relationship]
        >> create_feature_discovery_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
    )


datarobot_feature_discovery_pipeline_dag = datarobot_feature_discovery_pipeline()

if __name__ == "__main__":
    datarobot_feature_discovery_pipeline_dag.test()
