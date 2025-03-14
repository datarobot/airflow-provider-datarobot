# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from logging import Logger

import datarobot as dr
import pandas as pd
from airflow.decorators import dag

from datarobot_provider.operators.custom_function import CustomFunctionOperator
from datarobot_provider.operators.predictions import RequestPredictionsOperator

"""
Example of Aiflow DAG to run any custom function in a DAG, in this case a post processing function on predictions.
Configurable parameters for this dag:
* project_id - the project id for the project to use
* model_id - the model id for the model to use for doing the predictions
* dataset_id - the dataset id for the dataset to generate predictions for - either this or file_path must be provided
* file_path - the file path for the dataset to generate predictions for - either this or dataset_id must be provided
"""


def post_process_predictions(project_id: str, predict_job_id: str, log: Logger) -> str:
    """
    Manipulate predictions after they have been generated.
    Optionally, the function can accept the log parameter to log messages on the Airflow dashboard.
    """

    log.info(f"Fetching predictions for project {project_id} and predict job {predict_job_id}")
    predictions: pd.DataFrame = dr.PredictJob.get_predictions(
        project_id=project_id,
        predict_job_id=predict_job_id,
    )

    # Do some post-processing on the predictions here
    ...

    project = dr.Project.get(project_id)
    postprocessed_dataset = project.upload_dataset(
        sourcedata=predictions, dataset_filename=f"postprocessed-predictions-{predict_job_id}.csv"
    )

    return postprocessed_dataset.id


@dag(
    schedule=None,
    tags=["example", "custom_function"],
    params={
        "project_id": "YOUR_PROJECT_ID",
        "model_id": "YOUR_MODEL_ID",
        "dataset_id": "YOUR_DATASET_ID",
    },
)
def custom_function_dag():
    # Request predictions
    request_predictions = RequestPredictionsOperator(
        task_id="request_predictions",
        project_id="{{ params.project_id }}",
        model_id="{{ params.model_id }}",
        dataset_id="{{ params.dataset_id }}",
    )

    post_process = CustomFunctionOperator(
        task_id="my_function",
        custom_func=post_process_predictions,
        func_params={
            "project_id": "{{ params.project_id }}",
            "predict_job_id": str(request_predictions.output),
        },
    )

    (request_predictions >> post_process)


# Instantiate the DAG
custom_function_dag()
