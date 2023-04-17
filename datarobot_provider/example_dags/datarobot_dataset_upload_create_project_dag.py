# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "dataset_file_path": "/tests/integration/datasets/titanic.csv",
}
"""
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    tags=['example'],
    params={
        "dataset_file_path": "./titanic.csv",
        "project_name": "test project",
        "unsupervised_mode": False,
        "use_feature_discovery": False
    },
)
def datarobot_dataset_uploading_create_project():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    create_project_op = CreateProjectOperator(
        task_id='create_project',
        dataset_id=dataset_uploading_op.output,
    )

    dataset_uploading_op >> create_project_op


datarobot_pipeline_dag = datarobot_dataset_uploading_create_project()

if __name__ == "__main__":
    print(datarobot_pipeline_dag.test())
