# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.execution_environment import CreateExecutionEnvironmentOperator


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 1),
    tags=['example', 'custom model'],
    params={
        "execution_environment_name": "Demo Execution Environment",
        "execution_environment_description": "Demo Execution Environment for Airflow provider",
        "programming_language": "python",
        "required_metadata_keys": [{"field_name": "test_key", "display_name": "test_display_name"}],
    },
)
def create_execution_environment():
    create_execution_environment_op = CreateExecutionEnvironmentOperator(
        task_id='create_execution_environment',
    )

    create_execution_environment_op


create_execution_environment_dag = create_execution_environment()

if __name__ == "__main__":
    create_execution_environment_dag.test()
