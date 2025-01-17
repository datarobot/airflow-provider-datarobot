# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "deployment_id": "62cc0a6383d7a13d34f83344",
    "score_settings": {
        "intake_settings": {
            "type": "dataset",
            "dataset_id": "623d8ae79b186124a926c3cd"
        },
        "output_settings": {
            "type": "localFile",
            "path": "include/Diabetes_predictions.csv"
        }
    }
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.datarobot import ScorePredictionsOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=["example"],
)
def datarobot_score(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id=deployment_id,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    score_predictions_op >> scoring_complete_sensor


datarobot_pipeline_dag = datarobot_score()
