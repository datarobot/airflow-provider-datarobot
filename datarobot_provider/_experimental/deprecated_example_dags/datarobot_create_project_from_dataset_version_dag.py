# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Example of DAG to upload local file to DataRobot AI Catalog as a dataset version,
then using this dataset version as a training data to create a DataRobot Project.

Config example for this dag:
{
    "training_dataset_id": "existing training dataset id"
    "dataset_file_path": "/path/to/local/file",
    "project_name": "test project",
    "unsupervised_mode": False,
    "use_feature_discovery": False
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.data_registry import UpdateDatasetFromFileOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example"],
    params={
        "training_dataset_id": "644...590",
        "dataset_file_path": "/path/to/local/file",
        "project_name": "test project updated",
        "unsupervised_mode": False,
        "use_feature_discovery": False,
    },
)
def create_project_from_dataset_version():
    dataset_new_version_op = UpdateDatasetFromFileOperator(
        task_id="dataset_new_version",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project", dataset_version_id=dataset_new_version_op.output
    )

    dataset_new_version_op >> create_project_op


create_project_from_dataset_version_dag = create_project_from_dataset_version()
