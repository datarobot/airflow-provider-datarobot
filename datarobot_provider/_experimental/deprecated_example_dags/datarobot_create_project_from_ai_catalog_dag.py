# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Example of DAG to upload local file to DataRobot AICatalog as a static dataset,
then using this stored dataset as a training data to create a DataRobot Project.

Config example for this dag:
{
    "dataset_file_path": "/path/to/local/file",
    "project_name": "test project",
    "unsupervised_mode": False,
    "use_feature_discovery": False
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.data_registry import UploadDatasetOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example"],
    params={
        "dataset_file_path": "/path/to/local/file",
        "project_name": "test project",
        "unsupervised_mode": False,
        "use_feature_discovery": False,
    },
)
def create_project_from_aicatalog():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=dataset_uploading_op.output,
    )

    dataset_uploading_op >> create_project_op


create_project_from_aicatalog_dag = create_project_from_aicatalog()
