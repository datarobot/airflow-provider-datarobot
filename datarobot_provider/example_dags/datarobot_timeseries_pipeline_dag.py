# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from datarobot import AUTOPILOT_MODE

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example", "timeseries"],
    params={
        "dataset_file_path": "/dataset.csv",
        "project_name": "test airflow project timeseries",
        "autopilot_settings": {
            "target": "y",
            "mode": AUTOPILOT_MODE.QUICK,
        },
        "datetime_partitioning_settings": {
            "use_time_series": True,
            "datetime_partition_column": "datetime",
            "multiseries_id_columns": ["location"],
        },
        "unsupervised_mode": False,
        "use_feature_discovery": False,
    },
)
def datarobot_timeseries_pipeline():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=dataset_uploading_op.output,
    )

    train_models_op = StartAutopilotOperator(
        task_id="train_timeseries_models",
        project_id=create_project_op.output,
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project_op.output,
    )

    dataset_uploading_op >> create_project_op >> train_models_op >> autopilot_complete_sensor


datarobot_timeseries_pipeline_dag = datarobot_timeseries_pipeline()

if __name__ == "__main__":
    datarobot_timeseries_pipeline_dag.test()
