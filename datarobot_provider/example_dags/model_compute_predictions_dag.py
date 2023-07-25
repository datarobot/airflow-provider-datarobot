# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.model_predictions import AddExternalDatasetOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'dataset'],
)
def add_external_dataset(project_id=None, dataset_id=None):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not dataset_id:
        raise ValueError("Invalid or missing `dataset_id` value")

    add_external_dataset_op = AddExternalDatasetOperator(
        task_id="add_external_dataset",
        project_id=project_id,
        dataset_id=dataset_id,
    )

    add_external_dataset_op


add_external_dataset_dag = add_external_dataset()

if __name__ == "__main__":
    add_external_dataset_dag.test()
