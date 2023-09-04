# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime
import time
from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from datarobot_provider.operators.deployment import ActivateDeploymentOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'mlops'],
)
def deployment_deactivate_activate(deployment_id="64a7e4add4efa2b707e17daa"):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    deactivate_deployment_op = ActivateDeploymentOperator(
        task_id="deactivate_deployment",
        activate=False,
        deployment_id=deployment_id,
    )

    custom_python_op: PythonOperator = PythonOperator(
        task_id="custom_python_task",
        python_callable=lambda: time.sleep(600)
    )

    activate_deployment_op = ActivateDeploymentOperator(
        task_id="activate_deployment",
        activate=True,
        deployment_id=deployment_id,
    )

    deactivate_deployment_op >> custom_python_op >> activate_deployment_op


deployment_deactivate_activate_dag = deployment_deactivate_activate()

if __name__ == "__main__":
    deployment_deactivate_activate_dag.test()
