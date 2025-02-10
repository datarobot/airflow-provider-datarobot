# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Example of Aiflow DAG for DataRobot Batch Scoring using JDBC connection as source and destination,
and using a preconfigured "DataRobot JDBC DataSource" from Airflow Connection.
DataRobot JDBC DataSource can be configured using Airflow UI (Admin->Connections) or Airflow API
Config example for this dag:
{
    "deployment_id": "put_your_deployment_id_here",  # you can set deployment_id here
    "datarobot_jdbc_connection": "datarobot_jdbc_test_connection",
    "score_settings": {
        "intake_settings": {
            'type': 'jdbc',
            'table': 'input_table',
            'schema': 'input_table_schema',
        },
        "output_settings": {
            'type': 'jdbc',
            'schema': 'output_table_schema',
            'table': 'output_table',
            'statement_type': 'insert',
            'create_table_if_not_exists': True,
        },
        'passthrough_columns_set': 'all',
    },
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.connections import GetOrCreateDataStoreOperator
from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator
from datarobot_provider.operators.datarobot import ScorePredictionsOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "jdbc"],
    params={
        "deployment_id": "put_your_deployment_id_here",  # you can set deployment_id here
        "datarobot_jdbc_connection": "datarobot_jdbc_test_connection",
        "score_settings": {
            "intake_settings": {
                "type": "jdbc",
                "table": "input_table",
                "schema": "input_table_schema",
            },
            "output_settings": {
                "type": "jdbc",
                "schema": "output_table_schema",
                "table": "output_table",
                "statement_type": "insert",
                "create_table_if_not_exists": True,
            },
            "passthrough_columns_set": "all",
        },
    },
)
def datarobot_jdbc_batch_scoring(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    get_jdbc_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_jdbc_credentials",
        credentials_param_name="datarobot_jdbc_connection",
    )

    get_jdbc_connection_op = GetOrCreateDataStoreOperator(
        task_id="get_jdbc_connection",
        connection_param_name="datarobot_jdbc_connection",
    )

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions",
        deployment_id=deployment_id,
        intake_datastore_id=get_jdbc_connection_op.output,
        output_datastore_id=get_jdbc_connection_op.output,
        intake_credential_id=get_jdbc_credentials_op.output,
        output_credential_id=get_jdbc_credentials_op.output,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    (
        get_jdbc_connection_op
        >> get_jdbc_credentials_op
        >> score_predictions_op
        >> scoring_complete_sensor
    )


datarobot_jdbc_batch_scoring_dag = datarobot_jdbc_batch_scoring()

# Staring from Airflow 2.5.1 Debug Executor is deprecated,
# dag.test() should be used for dag testing:
if __name__ == "__main__":
    datarobot_jdbc_batch_scoring_dag.test()
