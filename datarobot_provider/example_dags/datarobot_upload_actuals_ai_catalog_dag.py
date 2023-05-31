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
    "datarobot_jdbc_connection": "datarobot_jdbc_test",
    "dataset_name": "test_dataset_name",
    "table_schema": "integration_demo",
    "table_name": "test_table",
    "query": 'SELECT * FROM "integration_demo"."test_table"',
    "persist_data_after_ingestion": True,
    "do_snapshot": True,
}
"""
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.mlops import UploadActualsOperator
from datarobot_provider.sensors.client import BaseAsyncResolutionSensor


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['example'],
    # Default json config example:
    params={
        "datarobot_jdbc_connection": "datarobot_jdbc_test_connection_name",
        "dataset_name": "test_jdbc_dataset_name",
        "table_schema": "test_jdbc_table_schema",
        "table_name": "test_jdbc_table_name",
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
    },
)
def datarobot_upload_actuals_from_ai_catalog():

    #dataset_connect_op = CreateDatasetFromDataStoreOperator(
    #    task_id="create_dataset_jdbc",
    #)

    upload_actuals_op = UploadActualsOperator(
        task_id='create_project',
        deployment_id='646fcfe9b01540a797f224b3',
        dataset_id='646fd3b7583f864e8a6c023e',
    )

    uploading_complete_sensor = BaseAsyncResolutionSensor(
        task_id="check_scoring_complete",
        async_location=upload_actuals_op.output,
        poke_interval=1,
        mode="reschedule",
        timeout=60,
    )

    upload_actuals_op >> uploading_complete_sensor


datarobot_upload_actuals_from_ai_catalog_dag = datarobot_upload_actuals_from_ai_catalog()

if __name__ == "__main__":
    datarobot_upload_actuals_from_ai_catalog_dag.test()
