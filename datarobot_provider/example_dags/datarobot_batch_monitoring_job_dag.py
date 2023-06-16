# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "deployment_id": "put_your_deployment_id_here",
    "datarobot_gcp_credentials": "demo_bigquery_test_credentials",
    "intake_settings": {
        "type": "bigquery",
        "dataset": "integration_demo",
        "table": "input_table_name",
        "bucket": "gcp_bucket_name",
    },
    "intake_settings": {
        "type": "jdbc",
        "data_store_id": "645043933d4fbc3215f17e35",
        "catalog": "SANDBOX",
        "table": "10kDiabetes_output_actuals",
        "schema": "SCORING_CODE_UDF_SCHEMA",
        "credential_id": "645043b61a158045f66fb328"
    },
    "monitoring_columns": {
        "predictions_columns": [
            {
                "class_name": "True",
                "column_name": "readmitted_True_PREDICTION"
            },
            {
                "class_name": "False",
                "column_name": "readmitted_False_PREDICTION"
            }
        ],
        "association_id_column": "rowID",
        "actuals_value_column": "ACTUALS"
    }
}
"""
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator
from datarobot_provider.operators.monitoring_job import BatchMonitoringOperator
from datarobot_provider.sensors.monitoring_job import MonitoringJobCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'gcp', 'bigquery'],
    params={
        "deployment_id": "646fcfe9b01540a797f224b3",
        "datarobot_gcp_credentials": "GCP_ai_engineering",
        "monitoring_settings": {
            "intake_settings": {
                "type": "bigquery",
                "dataset": "integration_example_demo",
                "table": "actuals_demo",
                "bucket": "datarobot_demo_airflow",
            },
            "monitoring_columns": {
                # "predictions_columns": [
                #     {
                #         "class_name": "True",
                #         "column_name": "readmitted_True_PREDICTION"
                #     },
                #     {
                #         "class_name": "False",
                #         "column_name": "readmitted_False_PREDICTION"
                #     }
                # ],
                "association_id_column": "id",
                "actuals_value_column": "ACTUAL",
            },
        },
    },
)
def datarobot_batch_monitoring(deployment_id='646fcfe9b01540a797f224b3'):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    get_bigquery_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_gcp_credentials",
        credentials_param_name="datarobot_gcp_credentials",
    )

    batch_monitoring_op = BatchMonitoringOperator(
        task_id="batch_monitoring",
        deployment_id=deployment_id,
        credential_id=get_bigquery_credentials_op.output,
    )

    batch_monitoring_complete_sensor = MonitoringJobCompleteSensor(
        task_id="check_monitoring_job_complete",
        job_id=batch_monitoring_op.output,
        poke_interval=1,
        mode="reschedule",
        timeout=3600,
    )

    (get_bigquery_credentials_op >> batch_monitoring_op >> batch_monitoring_complete_sensor)


datarobot_batch_monitoring_dag = datarobot_batch_monitoring()

if __name__ == "__main__":
    datarobot_batch_monitoring_dag.test()
