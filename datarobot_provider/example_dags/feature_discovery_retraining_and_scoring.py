# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from airflow.decorators import dag
from datarobot import AUTOPILOT_MODE

from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.data_prep import CreateFeatureDiscoveryRecipeOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromDataStoreOperator
from datarobot_provider.operators.data_registry import GetDataStoreOperator
from datarobot_provider.operators.data_registry import UploadDatasetOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import GetOrCreateUseCaseOperator
from datarobot_provider.operators.datarobot import SelectBestModelOperator
from datarobot_provider.operators.deployment import ReplaceModelOperator
from datarobot_provider.operators.model_registry import CreateRegisteredModelVersionOperator
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
        "deployment_id": "",
        "do_snapshot": False,
        "persist_data_after_ingestion": False,
        "data_connection": "Demo Connection",
        "table_schema": "TRIAL_READONLY",
        "table_name": "LENDING_CLUB_TRANSACTIONS",
        "dataset_name": "transactions",
    },
)
def datarobot_feature_discovery_retraining_and_scoring(
    primary_dataset_path=(
        "https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/LendingClub/target.csv"
    ),
    profile_dataset_path=(
        "https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/LendingClub/profile.csv"
    ),
):
    # Create a Use Case to keep all subsequent assets. Default name is "Airflow"
    create_use_case = GetOrCreateUseCaseOperator(task_id="create_use_case", set_default=True)

    # Upload primary dataset and a static secondary datasets into Data Registry.
    upload_primary_dataset = UploadDatasetOperator(
        task_id="upload_primary_dataset", file_path=primary_dataset_path
    )
    upload_profile_dataset = UploadDatasetOperator(
        task_id="upload_profile_dataset", file_path=profile_dataset_path
    )

    # Create a dynamic secondary dataset.
    get_data_store = GetDataStoreOperator(task_id="get_data_store")
    create_transactions_dataset = CreateDatasetFromDataStoreOperator(
        task_id="create_transactions_dataset", data_store_id=get_data_store.output
    )

    # Define the secondary datasets for Feature Discovery
    profile_dataset_definition = {
        "identifier": "profile",
        "catalogId": upload_profile_dataset.output,
    }

    transaction_dataset_definition = {
        "identifier": "transactions",
        "catalogId": create_transactions_dataset.output,
        "snapshotPolicy": "dynamic",
    }

    # Define the relationships between the datasets.
    # You do not need to specify dataset1Identifier when joining with the primary dataset.
    primary_profile_relationship = {
        "dataset2Identifier": "profile",  # to profile
        "dataset1Keys": ["CustomerID"],  # on CustomerID
        "dataset2Keys": ["CustomerID"],
    }

    profile_transaction_relationship = {
        "dataset1Identifier": "profile",  # join profile
        "dataset2Identifier": "transactions",  # to transactions
        "dataset1Keys": ["CustomerID"],  # on CustomerID
        "dataset2Keys": ["CustomerID"],
    }

    dataset_definitions = [profile_dataset_definition, transaction_dataset_definition]
    relationships = [primary_profile_relationship, profile_transaction_relationship]

    feature_discovery_settings = [
        {"name": "enable_days_from_prediction_point", "value": False},
        {"name": "enable_word_count", "value": False},
        {"name": "enable_pairwise_time_difference", "value": True},
        {"name": "enable_numeric_sum", "value": True},
    ]

    # Create a Feature Discovery recipe with all the above information.
    create_feature_discovery_recipe = CreateFeatureDiscoveryRecipeOperator(
        dataset_id=upload_primary_dataset.output,
        dataset_definitions=dataset_definitions,
        relationships=relationships,
        feature_discovery_settings=feature_discovery_settings,
        task_id="create_feature_discovery_recipe",
    )

    # Create and launch a project from the recipe.
    create_project = CreateProjectOperator(
        recipe_id=create_feature_discovery_recipe.output, task_id="create_project"
    )

    train_models = StartAutopilotOperator(task_id="train_models", project_id=create_project.output)

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete", project_id=create_project.output
    )

    # select best model
    select_best_model = SelectBestModelOperator(
        task_id="select_best_model",
        project_id=create_project.output,
        metric="RMSE",
    )
    # register model
    register_model = CreateRegisteredModelVersionOperator(
        task_id="register_model",
        model_version_params={
            "model_type": "leaderboard",
            "model_id": select_best_model.output,
            "name": "Highest readmitted score test",
            "registered_model_name": "Highest readmitted score test {{ ts }}",
        },
    )
    # replace model
    replace_model = ReplaceModelOperator(
        task_id="replace_model",
        new_registered_model_version_id=register_model.output,
        deployment_id="{{ params.deployment_id }}",
    )

    (
        create_use_case
        >> [
            upload_primary_dataset,
            upload_profile_dataset,
            get_data_store >> create_transactions_dataset,
        ]
        >> create_feature_discovery_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
        >> select_best_model
        >> register_model
        >> replace_model
    )


datarobot_feature_discovery_retraining_and_scoring()
