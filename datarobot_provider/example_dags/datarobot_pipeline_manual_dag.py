# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.autopilot import StartAutopilotOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import DeployRecommendedModelOperator
from datarobot_provider.operators.datarobot import ScorePredictionsOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor
from datarobot_provider.sensors.datarobot import ModelTrainingCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=['example'],
)
def datarobot_pipeline_manual():
    create_project_op = CreateProjectOperator(
        task_id='create_project',
    )

    train_models_op = StartAutopilotOperator(
        task_id="train_models",
        project_id=create_project_op.output,
    )

    model_training_complete_sensor = ModelTrainingCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project_op.output,
    )

    deploy_model_op = DeployRecommendedModelOperator(
        task_id="deploy_recommended_model",
        project_id=create_project_op.output,
    )

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id=deploy_model_op.output,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    (
        create_project_op
        >> train_models_op
        >> model_training_complete_sensor
        >> deploy_model_op
        >> score_predictions_op
        >> scoring_complete_sensor
    )


datarobot_pipeline_dag = datarobot_pipeline_manual()
