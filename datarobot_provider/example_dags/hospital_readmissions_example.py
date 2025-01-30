# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

import datarobot as dr

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator, \
    CreateWranglingRecipeOperator, CreateDatasetFromRecipeOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    tags=["example"],
    params={
        "dataset_file_path": "/path/to/10k_diabetes.csv",
        "project_name": "hospital-readmissions-example",
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
    },
)
def hospital_readmissions_example():
    dataset_uploading = UploadDatasetOperator(task_id="dataset_uploading")

    # get_connection = GetDataStoreOperator(task_id="get_connection")

    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        dataset_id=dataset_uploading.output,
        dialect=dr.enums.DataWranglingDialect.SPARK,
        operations=[
            {
              "directive": "drop-columns",
              "arguments": {
                "columns": [
                  "citoglipton",
                  "glipizide_metformin",
                  "glimepiride_pioglitazone",
                  "metformin_rosiglitazone",
                  "metformin_pioglitazone"
                ]
              }
            },
            {
              "directive": "replace",
              "arguments": {
                "origin": "admission_type_id",
                "searchFor": "",
                "replacement": "Not Available",
                "matchMode": "exact",
              }
            }
          ],
    )

    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe", recipe_id=create_recipe.output, do_snapshot=True
    )

    create_project = CreateProjectOperator(
        task_id="create_project", dataset_id=str(publish_recipe.output)
    )

    train_models = TrainModelsOperator(
        task_id="train_models",
        project_id=str(create_project.output),
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=str(create_project.output),
    )

    (dataset_uploading >> create_project >> train_models >> autopilot_complete_sensor)


hospital_readmissions_example()
