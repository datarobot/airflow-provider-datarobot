# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task

from datarobot_provider.operators.model_training import TrainModelOperator
from datarobot_provider.sensors.model_training import ModelTrainingJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "dataset"],
)
def train_model(
    project_id=None,
    blueprint_id=None,
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

    model_training_sensor = ModelTrainingJobSensor(
        task_id="model_training_complete",
        project_id=project_id,
        job_id=request_model_training_op.output,
        poke_interval=5,
        timeout=3600,
    )

    @task(task_id="example_custom_python_code")
    def using_custom_python_code(trained_model_id):
        """Example of using custom python code:"""
        logging.info(msg=f"New trained model id is: {trained_model_id}")

    example_custom_python_code = using_custom_python_code(
        trained_model_id=model_training_sensor.output
    )

    request_model_training_op >> model_training_sensor >> example_custom_python_code


train_model_dag = train_model()

if __name__ == "__main__":
    train_model_dag.test()
