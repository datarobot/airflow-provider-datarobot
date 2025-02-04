# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import CreateDatasetFromRecipeOperator
from datarobot_provider.operators.ai_catalog import CreateWranglingRecipeOperator
from datarobot_provider.operators.ai_catalog import UploadDatasetOperator

# from datarobot_provider.operators.connections import GetDataStoreOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import CreateUseCaseOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    tags=["example"],
    params={
        # "data_connection": "<YOUR DATAROBOT DATA CONNECTION NAME>",  # SNOWFLAKE (and any other database)
        # "table_schema": "<DB_SCHEMA>",  # SNOWFLAKE
        # "table_name": "<DB_TABLE>",  # SNOWFLAKE
        "dataset_file_path": "https://s3.amazonaws.com/datarobot_public_datasets/10k_diabetes.csv",  # CSV
        "project_name": "hospital-readmissions-example",
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
    },
)
def hospital_readmissions_example():
    create_use_case = CreateUseCaseOperator(task_id="create_use_case")

    # get_connection = GetDataStoreOperator(task_id="get_connection")  # SNOWFLAKE
    upload_dataset = UploadDatasetOperator(  # CSV
        task_id="upload_dataset", use_case_id=create_use_case.output
    )

    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        # data_store_id=get_connection.output,  # SNOWFLAKE
        # dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,  # SNOWFLAKE
        dataset_id=upload_dataset.output,  # CSV
        dialect=dr.enums.DataWranglingDialect.SPARK,  # CSV
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
                    # "searchFor": r"\[(\d+).*",  # SNOWFLAKE
                    # "replacement": r"\1",  # SNOWFLAKE
                    "searchFor": r"\\[(\\d+).*",  # CSV
                    "replacement": r"$1",  # CSV
                    "origin": "age",
                    "matchMode": "regex",
                },
            },
            {
                "directive": "compute-new",
                "arguments": {
                    # "expression": 'CAST("age" AS Integer)',  # SNOWFLAKE
                    "expression": "CAST(`age` AS Integer)",  # CSV
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
        use_case_id=create_use_case.output,
    )

    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe",
        recipe_id=create_recipe.output,
        do_snapshot=True,
        use_case_id=create_use_case.output,
    )

    create_project = CreateProjectOperator(
        task_id="create_project",
        dataset_id=str(publish_recipe.output),
        use_case_id=create_use_case.output,
    )

    train_models = TrainModelsOperator(
        task_id="train_models",
        project_id=str(create_project.output),
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=str(create_project.output),
    )

    (
        # [create_use_case, get_connection]  # SNOWFLAKE
        create_use_case  # CSV
        >> upload_dataset  # CSV
        >> create_recipe
        >> publish_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
    )


hospital_readmissions_example()
