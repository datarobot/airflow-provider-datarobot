# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Example of Aiflow DAG for DataRobot Batch Scoring using S3 as source and destination,
and using a preconfigured "DataRobot AWS Credentials" from Airflow Connections.
DataRobot AWS Credentials can be configured using Airflow UI (Admin->Connections) or Airflow API
Config example for this dag:
{
    "datarobot_aws_credentials": "demo_aws_s3_test_credentials",
    "deployment_id": "put_your_deployment_id",  # you can set deployment_id here
    "score_settings": {
        "intake_settings": {
            "type": "s3",
            "url": "s3://bucket_name/input_file_name.csv",
        },
        "output_settings": {
            "type": "s3",
            "url": "s3://bucket_name/output_file_name.csv",
        },
        # If passthrough columns are required, use this line:
        "passthrough_columns": ['column1', 'column2'],
    },
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator
from datarobot_provider.operators.datarobot import ScorePredictionsOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "aws"],
    params={
        "datarobot_aws_credentials": "demo_aws_s3_test_credentials",
        "deployment_id": "put_your_deployment_id",  # you can set deployment_id here
        "score_settings": {
            "intake_settings": {
                "type": "s3",
                "url": "s3://bucket_name/input_file_name.csv",
            },
            "output_settings": {
                "type": "s3",
                "url": "s3://bucket_name/output_file_name.csv",
            },
        },
    },
)
def datarobot_s3_batch_scoring(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    get_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_aws_credentials",
        credentials_param_name="datarobot_aws_credentials",
    )

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id=deployment_id,
        intake_credential_id=get_credentials_op.output,
        output_credential_id=get_credentials_op.output,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    get_credentials_op >> score_predictions_op >> scoring_complete_sensor


datarobot_s3_batch_scoring_dag = datarobot_s3_batch_scoring()

# Staring from Airflow 2.5.1 Debug Executor is deprecated,
# dag.test() should be used for dag testing:
if __name__ == "__main__":
    datarobot_s3_batch_scoring_dag.test()
