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

from datarobot_provider.operators.data_registry import UploadDatasetOperator


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example"],
    params={"dataset_file_path": "./titanic.csv"},
)
def datarobot_dataset_uploading():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    dataset_uploading_op


datarobot_pipeline_dag = datarobot_dataset_uploading()

if __name__ == "__main__":
    print(datarobot_pipeline_dag.test())
