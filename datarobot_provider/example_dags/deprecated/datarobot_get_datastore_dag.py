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
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.connections import GetOrCreateDataStoreOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    # Default json config example:
    params={
        "datarobot_jdbc_connection": "datarobot_jdbc_test",
    },
)
def datarobot_test_datastore():
    dataset_connect_op = GetOrCreateDataStoreOperator(
        task_id="create_dataset_jdbc",
        connection_param_name="datarobot_jdbc_connection",
    )

    dataset_connect_op


datarobot_test_datastore_dag = datarobot_test_datastore()

if __name__ == "__main__":
    datarobot_test_datastore_dag.test()
