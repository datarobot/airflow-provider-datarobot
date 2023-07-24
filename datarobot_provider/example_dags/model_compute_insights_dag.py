# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.model_insights import ComputeFeatureImpactOperator
from datarobot_provider.sensors.model_insights import ComputeFeatureImpactSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'insights'],
)
def compute_model_insights(
    project_id="64ba468e6390ef69973c97ab", model_id="64ba48f7d0a7b82c0ae5d4eb"
):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")

    compute_feature_impact_op = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact",
        project_id=project_id,
        model_id=model_id,
    )

    feature_impact_complete_sensor = ComputeFeatureImpactSensor(
        task_id="feature_impact_complete",
        project_id=project_id,
        job_id=compute_feature_impact_op.output,
        poke_interval=5,
        timeout=3600,
    )

    compute_feature_impact_op >> feature_impact_complete_sensor


compute_model_insights_dag = compute_model_insights()

if __name__ == "__main__":
    compute_model_insights_dag.test()
