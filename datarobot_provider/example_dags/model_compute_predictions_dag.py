# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task
from datarobot import PredictJob

from datarobot_provider.hooks.datarobot import DataRobotHook
from datarobot_provider.operators.model_predictions import AddExternalDatasetOperator
from datarobot_provider.operators.model_predictions import RequestModelPredictionsOperator
from datarobot_provider.sensors.model_insights import DataRobotJobSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "dataset"],
)
def compute_model_predictions(
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

    @task(task_id="example_custom_python_code")
    def using_custom_python_code(project_id, predict_job_id, datarobot_conn_id="datarobot_default"):
        """Example of using custom python code:"""

        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id).run()

        # Should be used only with a very small datasets (not higher than a few megabytes).
        output_df = PredictJob.get_predictions(project_id=project_id, predict_job_id=predict_job_id)

        # Put your logic with the model predictions output dataframe here, for example:
        return output_df["positive_probability"].mean() > 0.5

    example_custom_python_code = using_custom_python_code(
        project_id=project_id, predict_job_id=request_model_predictions_op.output
    )

    (
        add_external_dataset_op
        >> request_model_predictions_op
        >> model_predictions_sensor
        >> example_custom_python_code
    )


compute_model_predictions_dag = compute_model_predictions()

if __name__ == "__main__":
    compute_model_predictions_dag.test()
