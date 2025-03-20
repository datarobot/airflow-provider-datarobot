# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
from airflow.decorators import dag

from datarobot_provider.operators.data_prep import CreateWranglingRecipeOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromRecipeOperator
from datarobot_provider.operators.data_registry import CreateOrUpdateDataSourceOperator
from datarobot_provider.operators.data_registry import GetDataStoreOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import GetOrCreateUseCaseOperator
from datarobot_provider.operators.datarobot import GetProjectBlueprintsOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.operators.model_insights import ComputeShapPreviewOperator
from datarobot_provider.operators.model_registry import CreateRegisteredModelVersionOperator
from datarobot_provider.operators.model_training import TrainModelOperator
from datarobot_provider.sensors.model_training import ModelTrainingJobSensor

"""
Example of Aiflow DAG to apply db data transformations and train an xgboost model.
Configurable parameters for this dag:
* data_connection - database connection name you can find at https://app.datarobot.com/account/data-connections
* table_schema - the database schema to use
* primary_table - the table to start with
* secondary_table - the table to JOIN to the original table
* project_name - name of the project as displayed in DataRobot UI.
* autopilot_settings - a dictionary with the modelling project autopilot settings.
"""


@dag(
    schedule=None,
    tags=["example", "snowflake", "wrangling", "modeling"],
    params={
        "data_connection": "Demo Connection",
        "table_schema": "TRIAL_READONLY",
        "primary_table": "LENDING_CLUB_TRAINING",
        "secondary_table": "LENDING_CLUB_TRANSACTIONS",
        "project_name": "Lending Club",
        "autopilot_settings": {"target": "BadLoan", "mode": "manual", "max_wait": 3600},
    },
)
def model_training_xgboost():
    # Create a Use Case to keep all subsequent assets. Default name is "Airflow"
    create_use_case = GetOrCreateUseCaseOperator(task_id="create_use_case", set_default=True)

    # Define input data
    get_data_store = GetDataStoreOperator(task_id="get_data_store")
    define_transactions_table = CreateOrUpdateDataSourceOperator(
        task_id="define_transactions_table",
        data_store_id=get_data_store.output,
        table_name="{{ params.secondary_table }}",
    )

    # Define data preparation:
    # * Join `LENDING_CLUB_TRANSACTIONS` table,
    # * cast the `Amount` column to decimal
    # * and apply different types of aggregation.
    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        data_store_id=get_data_store.output,
        table_name="{{ params.primary_table }}",
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        operations=[
            {
                "directive": "join",
                "arguments": {
                    "leftKeys": ["CustomerID"],
                    "rightKeys": ["CustomerID"],
                    "joinType": "left",
                    "source": "table",
                    "rightDataSourceId": define_transactions_table.output,
                },
            },
            {
                "directive": "replace",
                "arguments": {
                    "origin": "Amount",
                    "searchFor": "[$,]",
                    "replacement": "",
                    "matchMode": "regex",
                    "isCaseSensitive": False,
                },
            },
            {
                "directive": "compute-new",
                "arguments": {
                    "expression": 'CAST("Amount" AS Decimal(10, 2))',
                    "newFeatureName": "decimal amount",
                },
            },
            {
                "directive": "aggregate",
                "arguments": {
                    "groupBy": ["CustomerID", "BadLoan", "date"],
                    "aggregations": [
                        {
                            "feature": "decimal amount",
                            "functions": ["sum", "avg", "stddev", "min", "max", "median"],
                        }
                    ],
                },
            },
        ],
    )

    # Apply data preparation and save the modified data in the Data Registry.
    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe",
        recipe_id=str(create_recipe.output),
        do_snapshot=True,
    )

    # Create a new Project.
    create_project = CreateProjectOperator(
        task_id="create_project",
        dataset_id=str(publish_recipe.output),
    )
    project_id = str(create_project.output)

    # Launch modeling autopilot in manual mode.
    start_modeling = TrainModelsOperator(task_id="train_models", project_id=project_id)

    # Get the blueprint id of an xgboost model.
    get_blueprint_id = GetProjectBlueprintsOperator(
        task_id="get_blueprint_id",
        project_id=project_id,
        filter_model_type="extreme gradient boosted",
    )

    trained_model = TrainModelOperator(
        task_id="train_model",
        project_id=project_id,
        blueprint_id=str(get_blueprint_id.output),
    )

    trained_model_sensor = ModelTrainingJobSensor(
        task_id="model_training_complete",
        project_id=project_id,
        job_id=str(trained_model.output),
        poke_interval=5,
        timeout=3600,
    )

    insights = ComputeShapPreviewOperator(
        task_id="compute_shap_insights",
        model_id=str(trained_model_sensor.output),
    )

    # register model
    registered_model_name = f"Highest readmitted score {str(trained_model_sensor.output)}"
    register_model = CreateRegisteredModelVersionOperator(
        task_id="register_model",
        model_version_params={
            "model_type": "leaderboard",
            "model_id": str(trained_model_sensor.output),
            "name": registered_model_name,
            "registered_model_name": registered_model_name,
        },
    )

    (
        create_use_case
        >> get_data_store
        >> define_transactions_table
        >> create_recipe
        >> publish_recipe
        >> create_project
        >> start_modeling
        >> get_blueprint_id
        >> trained_model
        >> trained_model_sensor
        >> insights
        >> register_model
    )


model_training_xgboost()
