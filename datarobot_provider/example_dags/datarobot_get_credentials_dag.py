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
    "datarobot_credentials_name": "datarobot_basic_credentials_test",
}
"""
from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.credentials import GetCredentialIdOperator


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['example'],
    # Default json config:
    params={
        "datarobot_credentials_name": "datarobot_basic_credentials_test",
    },
)
def datarobot_get_credentials():
    get_credentials_op = GetCredentialIdOperator(
        task_id="get_credentials",
    )

    get_credentials_op


datarobot_get_credential_dag = datarobot_get_credentials()

if __name__ == "__main__":
    print(datarobot_get_credential_dag.test())
