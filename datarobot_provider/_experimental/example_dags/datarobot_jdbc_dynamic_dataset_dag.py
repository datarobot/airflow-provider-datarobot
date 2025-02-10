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
    "persist_data_after_ingestion": False,
    "do_snapshot": False,
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import CreateDatasetFromDataStoreOperator
from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    # Default json config example:
    params={
        "project_name": "test_project_name",
        "datarobot_jdbc_connection": "datarobot_jdbc_test_connection_name",
        "dataset_name": "test_jdbc_dataset_name",
        "table_schema": "test_jdbc_table_schema",
        "table_name": "test_jdbc_table_name",
        "persist_data_after_ingestion": False,
        "do_snapshot": False,
    },
)
def datarobot_dynamic_jdbc_dataset():
    dataset_connect_op = CreateDatasetFromDataStoreOperator(
        task_id="create_dataset_jdbc",
    )

    # In case of dynamic dataset we should provide credential_id from connection
    get_jdbc_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_jdbc_credentials",
        credentials_param_name="datarobot_jdbc_connection",
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=dataset_connect_op.output,
        # In case of dynamic dataset we should provide credential_id
        credential_id=get_jdbc_credentials_op.output,
    )

    dataset_connect_op >> create_project_op


datarobot_dynamic_jdbc_dataset_dag = datarobot_dynamic_jdbc_dataset()

if __name__ == "__main__":
    datarobot_dynamic_jdbc_dataset_dag.test()
