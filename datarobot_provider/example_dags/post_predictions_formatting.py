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

"""
Example of Aiflow DAG to run any custom function in a DAG, in this case a post processing function on predictions.
Configurable parameters for this dag:
* project_id - the project id for the project to use
* predict_job_id - the predict job id for the predictions to post-process
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

    # Do some post-processing on the predictions dataframe here
    ...

    project = dr.Project.get(project_id)
    postprocessed_dataset = project.upload_dataset(
        sourcedata=predictions, dataset_filename=f"postprocessed-predictions-{predict_job_id}.csv"
    )

    log.info(f"Post-processed predictions saved to dataset {postprocessed_dataset.id}")

    return postprocessed_dataset.id


@dag(
    schedule=None,
    tags=["example", "custom_function", "post_predictions_formatting"],
    params={
        "project_id": "YOUR_PROJECT_ID",
        "predict_job_id": "YOUR_PREDICT_JOB_ID",
    },
)
def post_predictions_formatting_dag():
    post_process = CustomFunctionOperator(
        task_id="custom_post_process_function",
        # This can be any arbitrary function
        custom_func=post_process_predictions,
        # The parameters must be specified here as a dictionary
        func_params={
            "project_id": "{{ params.project_id }}",
            "predict_job_id": "{{ params.predict_job_id }}",
        },
    )

    (post_process)


# Instantiate the DAG
post_predictions_formatting_dag()
