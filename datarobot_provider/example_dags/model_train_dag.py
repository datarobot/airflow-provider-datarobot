# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task
from datarobot import PredictJob

from datarobot_provider.hooks.datarobot import DataRobotHook
from datarobot_provider.operators.model_predictions import (
    AddExternalDatasetOperator,
    RequestModelPredictionsOperator,
)
from datarobot_provider.operators.model_training import TrainModelOperator
from datarobot_provider.sensors.model_insights import DataRobotJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'dataset'],
)
def train_model(
    project_id='64be056fdcee417faf6ec832',
    blueprint_id='27f50e490d5cf54850a231ce7f1844a4',
):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not blueprint_id:
        raise ValueError("Invalid or missing `blueprint_id` value")

    request_model_training_op = TrainModelOperator(
        task_id="request_model_training",
        project_id=project_id,
        blueprint_id=blueprint_id,
    )

    model_training_sensor = DataRobotJobSensor(
        task_id="model_training_complete",
        project_id=project_id,
        job_id=request_model_training_op.output,
        poke_interval=5,
        timeout=3600,
    )

    request_model_training_op >> model_training_sensor


train_model_dag = train_model()

if __name__ == "__main__":
    train_model_dag.test()
