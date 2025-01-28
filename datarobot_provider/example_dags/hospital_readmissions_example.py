# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example"],
    params={
        "dataset_file_path": "/path/to/10k_diabetes.csv",
        "project_name": "hospital-readmissions-example",
        "unsupervised_mode": False,
        "use_feature_discovery": False,
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
    },
)
def hospital_readmissions_example():
    dataset_uploading_op = UploadDatasetOperator(
        task_id="dataset_uploading",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project", dataset_id=str(dataset_uploading_op.output)
    )

    train_models_op = TrainModelsOperator(
        task_id="train_models",
        project_id=str(create_project_op.output),
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=str(create_project_op.output),
    )

    (dataset_uploading_op >> create_project_op >> train_models_op >> autopilot_complete_sensor)


hospital_readmissions_example = hospital_readmissions_example()  # type: ignore[assignment]
