# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

import datarobot as dr
from airflow.decorators import dag
from airflow.decorators import task

from datarobot_provider.hooks.datarobot import DataRobotHook
from datarobot_provider.operators.model_insights import ComputeFeatureImpactOperator
from datarobot_provider.operators.model_predictions import AddExternalDatasetOperator
from datarobot_provider.operators.model_predictions import RequestModelPredictionsOperator
from datarobot_provider.operators.prediction_explanations import (
    ComputePredictionExplanationsOperator,
)
from datarobot_provider.operators.prediction_explanations import (
    PredictionExplanationsInitializationOperator,
)
from datarobot_provider.sensors.model_insights import DataRobotJobSensor

"""
Example DAG to create prediction explanations for the specified dataset.
In order to create Prediction Explanations for a particular model and dataset, you must use the next steps:
 - Compute feature impact for the model using ComputeFeatureImpactOperator
 - Initialize prediction explanations for the model using PredictionExplanationsInitializationOperator
 - Compute predictions for the model and dataset using RequestModelPredictionsOperator
 - Compute predictions explanations for the model and dataset using ComputePredictionExplanationsOperator
"""


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "dataset", "model"],
    # Default json config example:
    params={"threshold_high": 0.9, "threshold_low": 0.1, "max_explanations": 3},
)
def compute_model_prediction_explanations(
    project_id=None,
    model_id=None,
    dataset_id=None,
):
    if not project_id:
        raise ValueError("Invalid or missing `project_id` value")
    if not model_id:
        raise ValueError("Invalid or missing `model_id` value")
    if not dataset_id:
        raise ValueError("Invalid or missing `dataset_id` value")

    add_external_dataset_op = AddExternalDatasetOperator(
        task_id="add_external_dataset",
        project_id=project_id,
        dataset_id=dataset_id,
    )

    compute_feature_impact_op = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact",
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

    request_model_predictions_op = RequestModelPredictionsOperator(
        task_id="request_model_predictions",
        project_id=project_id,
        model_id=model_id,
        external_dataset_id=add_external_dataset_op.output,
    )

    model_predictions_sensor = DataRobotJobSensor(
        task_id="model_predictions_complete",
        project_id=project_id,
        job_id=request_model_predictions_op.output,
        poke_interval=5,
        timeout=3600,
    )

    # In order to compute prediction explanations you have to initialize it for a particular model.
    prediction_explanations_initialization_op = PredictionExplanationsInitializationOperator(
        task_id="prediction_explanations_initialization",
        project_id=project_id,
        model_id=model_id,
    )

    prediction_explanations_initialization_sensor = DataRobotJobSensor(
        task_id="prediction_explanations_initialization_complete",
        project_id=project_id,
        job_id=prediction_explanations_initialization_op.output,
        poke_interval=5,
        timeout=3600,
    )

    compute_prediction_explanations_op = ComputePredictionExplanationsOperator(
        task_id="compute_prediction_explanations",
        project_id=project_id,
        model_id=model_id,
        external_dataset_id=add_external_dataset_op.output,
    )

    compute_prediction_explanations_sensor = DataRobotJobSensor(
        task_id="compute_prediction_explanations_complete",
        project_id=project_id,
        job_id=compute_prediction_explanations_op.output,
        poke_interval=5,
        timeout=3600,
    )

    @task(task_id="example_custom_python_code")
    def using_custom_python_code(
        pe_project_id, pe_model_id, predict_job_id, datarobot_conn_id="datarobot_default"
    ):
        """Example of using custom python code:"""

        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id).run()

        # Should be used only with a very small datasets (not higher than a few megabytes).
        predictions_df = dr.PredictJob.get_predictions(
            project_id=pe_project_id, predict_job_id=predict_job_id
        )
        # Get last prediction explanations computation
        explanations_id = dr.PredictionExplanations.list(pe_project_id, model_id=pe_model_id)[-1].id
        prediction_explanations_df = dr.PredictionExplanations.get(
            pe_project_id, explanations_id
        ).get_all_as_dataframe()

        # Put your logic with the model predictions output and prediction explanations here, for example:
        return predictions_df["positive_probability"].mean(), prediction_explanations_df[
            "explanation_0_feature"
        ].describe().get("top")

    example_custom_python_code = using_custom_python_code(
        pe_project_id=project_id,
        pe_model_id=model_id,
        predict_job_id=request_model_predictions_op.output,
    )

    (
        add_external_dataset_op
        >> compute_feature_impact_op
        >> feature_impact_complete_sensor
        >> prediction_explanations_initialization_op
        >> prediction_explanations_initialization_sensor
        >> request_model_predictions_op
        >> model_predictions_sensor
        >> compute_prediction_explanations_op
        >> compute_prediction_explanations_sensor
        >> example_custom_python_code
    )


compute_model_prediction_explanations_dag = compute_model_prediction_explanations()

if __name__ == "__main__":
    compute_model_prediction_explanations_dag.test()
