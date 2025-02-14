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
from datarobot import SCORING_TYPE

from datarobot_provider.operators.model_training import RetrainModelOperator
from datarobot_provider.sensors.model_training import ModelTrainingJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "retrain"],
    # Default json config example:
    params={
        "sample_pct": 60,  # training dataset sample size to retrain the model.
        "scoring_type": SCORING_TYPE.cross_validation,  # validation type to retrain the model.
    },
)
def retrain_model(
    project_id=None,
    model_id=None,
):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")

    request_model_retraining_op = RetrainModelOperator(
        task_id="request_model_retraining",
        project_id=project_id,
        model_id=model_id,
    )

    model_retraining_sensor = ModelTrainingJobSensor(
        task_id="model_retraining_complete",
        project_id=project_id,
        job_id=request_model_retraining_op.output,
        poke_interval=5,
        timeout=3600,
    )

    @task(task_id="example_custom_python_code")
    def using_custom_python_code(retrained_model_id):
        """Example of using custom python code:"""
        logging.info(msg=f"New retrained model id is: {retrained_model_id}")

    example_custom_python_code = using_custom_python_code(
        retrained_model_id=model_retraining_sensor.output
    )

    request_model_retraining_op >> model_retraining_sensor >> example_custom_python_code


retrain_model_dag = retrain_model()

if __name__ == "__main__":
    retrain_model_dag.test()
