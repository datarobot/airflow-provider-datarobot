# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.model_insights import ComputeFeatureEffectsOperator
from datarobot_provider.operators.model_insights import ComputeFeatureImpactOperator
from datarobot_provider.sensors.model_insights import DataRobotJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "insights"],
)
def compute_model_insights(project_id=None, model_id=None):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")

    compute_feature_impact_op = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact",
        project_id=project_id,
        model_id=model_id,
    )

    compute_feature_effects_op = ComputeFeatureEffectsOperator(
        task_id="compute_feature_effects",
        project_id=project_id,
        model_id=model_id,
    )

    feature_impact_complete_sensor = DataRobotJobSensor(
        task_id="feature_impact_complete",
        project_id=project_id,
        job_id=compute_feature_impact_op.output,
        poke_interval=5,
        timeout=3600,
    )

    feature_feature_effects_sensor = DataRobotJobSensor(
        task_id="feature_effects_complete",
        project_id=project_id,
        job_id=compute_feature_effects_op.output,
        poke_interval=5,
        timeout=3600,
    )

    compute_feature_impact_op >> feature_impact_complete_sensor
    compute_feature_effects_op >> feature_feature_effects_sensor


compute_model_insights_dag = compute_model_insights()

if __name__ == "__main__":
    compute_model_insights_dag.test()
