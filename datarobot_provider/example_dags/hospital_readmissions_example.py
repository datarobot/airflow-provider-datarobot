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
from datarobot_provider.operators.connections import GetDataStoreOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import CreateUseCaseOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    tags=["example"],
    params={
        # "dataset_file_path": "/path/to/10k_diabetes.csv",
        "data_connection": "<YOUR DATAROBOT DATA CONNECTION NAME>",
        "table_schema": "<DB_SCHEMA>",
        "table_name": "<DB_TABLE>",
        "project_name": "hospital-readmissions-example",
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
    },
)
def hospital_readmissions_example():
    create_use_case = CreateUseCaseOperator(task_id="create_use_case")

    # upload_dataset = UploadDatasetOperator(task_id="upload_dataset")
    get_connection = GetDataStoreOperator(task_id="get_connection")

    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        # Database data preparation.
        data_store_id=get_connection.output,
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        # CSV data preparation.
        # dataset_id=upload_dataset.output,
        # dialect=dr.enums.DataWranglingDialect.SPARK,
        operations=[
            {
                "directive": "drop-columns",
                "arguments": {
                    "columns": [
                        "citoglipton",
                    ]
                },
            },
            {
                "directive": "replace",
                "arguments": {
                    "origin": "admission_type_id",
                    "searchFor": "",
                    "replacement": "Not Available",
                    "matchMode": "exact",
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
        [create_use_case, get_connection]
        >> create_recipe
        >> publish_recipe
        >> create_project
        >> train_models
        >> autopilot_complete_sensor
    )


hospital_readmissions_example()
