# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator

from datarobot_provider.operators.deployment import ActivateDeploymentOperator
from datarobot_provider.operators.deployment import GetDeploymentStatusOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
)
def deployment_deactivate_activate(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    get_deployment_status_initial_op = GetDeploymentStatusOperator(
        task_id="get_deployment_status_initial",
        deployment_id=deployment_id,
    )

    def choose_branch(deployment_status):
        if deployment_status == "inactive":
            return ["activate_deployment"]
        return ["deactivate_deployment"]

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        op_args=[get_deployment_status_initial_op.output],
    )

    deactivate_deployment_op = ActivateDeploymentOperator(
        task_id="deactivate_deployment",
        activate=False,
        deployment_id=deployment_id,
    )

    activate_deployment_op = ActivateDeploymentOperator(
        task_id="activate_deployment",
        activate=True,
        deployment_id=deployment_id,
    )

    (
        get_deployment_status_initial_op
        >> branching
        >> (activate_deployment_op, deactivate_deployment_op)
    )


deployment_deactivate_activate_dag = deployment_deactivate_activate()

if __name__ == "__main__":
    deployment_deactivate_activate_dag.test()
