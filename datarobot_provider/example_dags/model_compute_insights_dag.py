# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.model_insights import ComputePredictionExplanationsOperator
from datarobot_provider.sensors.model_insights import ComputePredictionExplanationsSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'insights'],
)
def compute_prediction_explanations(
    project_id="64ba468e6390ef69973c97ab", model_id="64ba48f7d0a7b82c0ae5d4eb"
):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")

    compute_prediction_explanations_op = ComputePredictionExplanationsOperator(
        task_id="compute_prediction_explanations",
        project_id=project_id,
        model_id=model_id,
    )

    prediction_explanations_complete_sensor = ComputePredictionExplanationsSensor(
        task_id="prediction_explanations_complete",
        project_id=project_id,
        job_id=compute_prediction_explanations_op.output,
        poke_interval=5,
        timeout=3600,
    )

    compute_prediction_explanations_op >> prediction_explanations_complete_sensor


compute_prediction_explanations_dag = compute_prediction_explanations()

if __name__ == "__main__":
    compute_prediction_explanations_dag.test()
