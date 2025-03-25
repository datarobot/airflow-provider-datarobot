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
from datarobot_provider.operators.data_registry import UploadDatasetOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import GetOrCreateUseCaseOperator
from datarobot_provider.operators.datarobot import SelectBestModelOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.operators.model_registry import CreateRegisteredModelVersionOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor

"""
Example of Aiflow DAG for DataRobot data preparation and model training.
Configurable parameters for this dag:
* dataset_file_path - URL or a local path for a csv/parquet/xlsx file.
* project_name - name of the project as displayed in DataRobot UI.
* autopilot_settings - a dictionary with the modelling project autopilot settings.
"""


@dag(
    schedule=None,
    tags=["example", "csv", "wrangling", "modeling"],
    params={
        "dataset_file_path": "https://s3.amazonaws.com/datarobot_public_datasets/10k_diabetes.csv",
        "project_name": "hospital-readmissions-example",
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
    },
)
def dataprep_wrangler_autopilot():
    # Create a Use Case to keep all subsequent assets. Default name is "Airflow"
    create_use_case = GetOrCreateUseCaseOperator(task_id="create_use_case", set_default=True)

    # Upload the data into Data Registry.
    upload_dataset = UploadDatasetOperator(task_id="upload_dataset")

    # Define data preparation.
    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        dataset_id=upload_dataset.output,
        dialect=dr.enums.DataWranglingDialect.SPARK,
        # See the list of available *operation* options in the DtaRobot API documentation:
        # https://docs.datarobot.com/en/docs/api/reference/public-api/data_wrangling.html#schemaoneofdirective
        # General *operation* structure is:
        # {"directive": <One of dr.enums.WranglingOperations>, "arguments": <dictionary>}
        operations=[
            {
                "directive": "rename-columns",
                "arguments": {
                    "columnMappings": [
                        {"originalName": "admission_type_id", "newName": "AdmissionType"},
                        {
                            "originalName": "discharge_disposition_id",
                            "newName": "DischargeDisposition",
                        },
                        {"originalName": "admission_source_id", "newName": "AdmissionSource"},
                    ]
                },
            },
            {
                "directive": "replace",
                "arguments": {
                    "origin": "AdmissionType",
                    "searchFor": "",
                    "replacement": "Not Available",
                    "matchMode": "exact",
                },
            },
            {
                "directive": "replace",
                "arguments": {
                    "searchFor": r"\\[(\\d+).*",
                    "replacement": r"$1",
                    "origin": "age",
                    "matchMode": "regex",
                },
            },
            {
                "directive": "compute-new",
                "arguments": {
                    "expression": "CAST(`age` AS Integer)",
                    "newFeatureName": "int-age",
                },
            },
            {
                "directive": "drop-columns",
                "arguments": {
                    "columns": [
                        "citoglipton",
                        "acetohexamide",
                        "miglitol",
                        "troglitazone",
                        "examide",
                        "age",
                    ]
                },
            },
        ],
    )

    # Apply data preparation and save the modified data in the Data Registry.
    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe",
        recipe_id=create_recipe.output,
        do_snapshot=True,
    )

    # Create a new Project.
    create_project = CreateProjectOperator(
        task_id="create_project",
        dataset_id=publish_recipe.output,
    )

    # Launch modeling autopilot.
    train_models = TrainModelsOperator(task_id="train_models", project_id=create_project.output)

    # Wait for the autopilot completion.
    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project.output,
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

    (
        create_use_case
        >> upload_dataset
        >> create_recipe
        >> publish_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
        >> select_best_model
        >> register_model
    )


dataprep_wrangler_autopilot()
