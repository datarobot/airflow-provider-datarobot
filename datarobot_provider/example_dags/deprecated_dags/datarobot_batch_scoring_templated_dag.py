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
    tags=["example", "scoring"],
)
def datarobot_batch_scoring_templated():
    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id="testdeploymentid",
        score_settings={
            "intake_settings": {"type": "dataset", "dataset_id": "testdatasetid"},
            "output_settings": {
                "type": "localFile",
                "path": "include/{{ ds_nodash }}/{{ params.myparam }}/Diabetes_predictions.csv",
            },
        },
        # custom parameter example
        params={"myparam": "test_param_value"},
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    score_predictions_op >> scoring_complete_sensor


datarobot_batch_scoring_templated_dag = datarobot_batch_scoring_templated()

if __name__ == "__main__":
    datarobot_batch_scoring_templated_dag.test()
