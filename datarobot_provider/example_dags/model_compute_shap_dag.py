# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.model_insights import ComputeShapOperator
from datarobot_provider.sensors.model_insights import DataRobotJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "insights"],
)
def compute_model_shap(project_id=None, model_id=None):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")

    compute_shap_op = ComputeShapOperator(
        task_id="compute_feature_impact",
        project_id=project_id,
        model_id=model_id,
    )

    shap_complete_sensor = DataRobotJobSensor(
        task_id="feature_impact_complete",
        project_id=project_id,
        job_id=compute_shap_op.output,
        poke_interval=5,
        timeout=3600,
    )

    compute_shap_op >> shap_complete_sensor


compute_model_shap_dag = compute_model_shap()

if __name__ == "__main__":
    compute_model_shap_dag.test()
