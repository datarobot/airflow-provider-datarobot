# Copyright 2024 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.custom_job import CreateCustomJobOperator
from datarobot_provider.operators.custom_job import AddFilesToCustomJobOperator
from datarobot_provider.operators.custom_job import SetCustomJobExecutionEnvironmentOperator
from datarobot_provider.operators.custom_job import SetCustomJobRuntimeParametersOperator
from datarobot_provider.operators.custom_job import RunCustomJobOperator
from datarobot_provider.sensors.client import BaseAsyncResolutionSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'custom job'],
    params={},
)
def create_custom_custom_job():
    create_custom_job_op = CreateCustomJobOperator(
        task_id='create_custom_job',
        name="airflow-test-create-custom-job-v556",
        description="demo-test-demonstration",
    )

    add_files_to_custom_job_op = AddFilesToCustomJobOperator(
        task_id='add_files_to_custom_job',
        custom_job_id=create_custom_job_op.output,
        files_path="custom_job/",
    )

    # list_execution_env_op = ListExecutionEnvironmentOperator(
    #     task_id='list_execution_env',
    #     search_for="Python 3.9 PyTorch Drop-In"
    # )

    set_env_to_custom_job_op = SetCustomJobExecutionEnvironmentOperator(
        task_id='set_env_to_custom_job',
        custom_job_id=create_custom_job_op.output,
        environment_id='5e8c888007389fe0f466c72b',
        environment_version_id='65c1db901800cd9782d7ac07',
    )

    set_runtime_parameters_op = SetCustomJobRuntimeParametersOperator(
        task_id='set_runtime_parameters',
        custom_job_id=create_custom_job_op.output,
        runtime_parameter_values=[
            {"fieldName": "DEPLOYMENT", "type": "deployment", "value": "650ef15944f21ea1a3c91a25"},
            {
                "fieldName": "MODEL_PACKAGE",
                "type": "modelPackage",
                "value": "654b9b228404a39b5c8da5b2",
            },
            {"fieldName": "STRING_PARAMETER", "type": "string", "value": 'my test string'},
        ],
    )

    run_custom_job_op = RunCustomJobOperator(
        task_id='run_custom_job',
        custom_job_id=create_custom_job_op.output,
    )

    custom_job_complete_sensor = BaseAsyncResolutionSensor(
        task_id="check_custom_job_complete",
        job_id=run_custom_job_op.output,
        poke_interval=5,
        mode="reschedule",
        timeout=3600,
    )

    (
        create_custom_job_op
        >> add_files_to_custom_job_op
        >> set_env_to_custom_job_op
        >> set_runtime_parameters_op
        >> run_custom_job_op
        >> custom_job_complete_sensor
    )


create_custom_job_dag = create_custom_custom_job()

if __name__ == "__main__":
    create_custom_job_dag.test()
