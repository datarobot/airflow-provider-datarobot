# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Example of Aiflow DAG for DataRobot Batch Scoring using Google Cloud Storage as source and destination,
and using a preconfigured "DataRobot GCP Credentials" from Airflow Connection.
DataRobot GCP Credentials can be configured using Airflow UI (Admin->Connections) or Airflow API
Config example for this dag:
{
    "datarobot_gcp_credentials": "your_gcp_credentials_name",
    "deployment_id": "put_your_deployment_id",  # you can set deployment_id here
    "score_settings": {
        "intake_settings": {
            "type": "gcp",
            "url": "gs://bucket_name/input_file_name.csv",
        },
        "output_settings": {
            "type": "gcp",
            "url": "gs://bucket_name/output_file_name.csv",
        },
        # If passthrough columns are required, use this line:
        "passthrough_columns": ['column1', 'column2'],
    },
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator
from datarobot_provider.operators.deployment import ScorePredictionsOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "gcp"],
    params={
        "datarobot_gcp_credentials": "your_gcp_credentials_name",
        "deployment_id": "put_your_deployment_id",  # you can set deployment_id here
        "score_settings": {
            "intake_settings": {
                "type": "gcp",
                "url": "gs://bucket_name/input_file_name.csv",
            },
            "output_settings": {
                "type": "gcp",
                "url": "gs://bucket_name/output_file_name.csv",
            },
            # If passthrough columns are required, use this line:
            "passthrough_columns": ["column1", "column2"],
        },
    },
)
def datarobot_gcp_batch_scoring(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    get_gcp_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_gcp_credentials",
        credentials_param_name="datarobot_gcp_credentials",
    )

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id=deployment_id,
        intake_credential_id=get_gcp_credentials_op.output,
        output_credential_id=get_gcp_credentials_op.output,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    get_gcp_credentials_op >> score_predictions_op >> scoring_complete_sensor


datarobot_gcp_batch_scoring_dag = datarobot_gcp_batch_scoring()

# Staring from Airflow 2.5.1 Debug Executor is deprecated,
# dag.test() should be used for dag testing:
if __name__ == "__main__":
    datarobot_gcp_batch_scoring_dag.test()
