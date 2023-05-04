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

from datarobot_provider.operators.ai_catalog import CreateDatasetFromJDBCOperator


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['example'],
    # Default json config example:
    params={
        "datarobot_jdbc_connection": "datarobot_jdbc_default",
        "dataset_name": "test_dataset_name",
        "table_schema": "integration_demo",
        "table_name": "test_table",
        "persist_data_after_ingestion": True,
        "do_snapshot": True,
    },
)
def datarobot_dataset_connect():
    dataset_connect_op = CreateDatasetFromJDBCOperator(
        task_id="create_dataset_jdbc",
    )

    dataset_connect_op


datarobot_jdbc_connection_dag = datarobot_dataset_connect()

if __name__ == "__main__":
    print(datarobot_jdbc_connection_dag.test())
