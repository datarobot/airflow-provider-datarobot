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
from datarobot.enums import DATETIME_AUTOPILOT_DATA_SELECTION_METHOD
from datarobot.helpers.partitioning_methods import construct_duration_string

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example", "datetime_partitioning"],
    params={
        "dataset_file_path": "/train_datetime.csv",
        "project_name": "test airflow datetime-partitioning project",
        "autopilot_settings": {
            "target": "y",
            "mode": AUTOPILOT_MODE.QUICK,
        },
        "datetime_partitioning_settings": {
            "use_time_series": False,
            "datetime_partition_column": "datetime",
            "number_of_backtests": 1,
            "autopilot_data_selection_method": DATETIME_AUTOPILOT_DATA_SELECTION_METHOD.DURATION,
            "validation_duration": construct_duration_string(years=1),
            "gap_duration": construct_duration_string(days=1),
        },
        "unsupervised_mode": False,
        "use_feature_discovery": False,
    },
)
def datarobot_datetime_partitioning_pipeline():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=dataset_uploading_op.output,
    )

    train_models_op = StartAutopilotOperator(
        task_id="train_otv_models",
        project_id=create_project_op.output,
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project_op.output,
    )

    dataset_uploading_op >> create_project_op >> train_models_op >> autopilot_complete_sensor


datarobot_datetime_partitioning_pipeline_dag = datarobot_datetime_partitioning_pipeline()

if __name__ == "__main__":
    datarobot_datetime_partitioning_pipeline_dag.test()
