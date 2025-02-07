# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task
from datarobot.enums import MODEL_REPLACEMENT_REASON

from datarobot_provider.operators.deployment import GetDeploymentModelOperator
from datarobot_provider.operators.deployment import ReplaceModelOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
)
def deployment_replace_model(deployment_id=None, new_model_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")
    if not new_model_id:
        raise ValueError("Invalid or missing `new_model_id` value")

    get_deployment_model_before_op = GetDeploymentModelOperator(
        task_id="get_deployment_model_before",
        deployment_id=deployment_id,
    )

    replace_deployment_model_op = ReplaceModelOperator(
        task_id="replace_deployment_model",
        deployment_id=deployment_id,
        new_model_id=new_model_id,
        reason=MODEL_REPLACEMENT_REASON.ACCURACY,
    )

    get_deployment_model_after_op = GetDeploymentModelOperator(
        task_id="get_deployment_model_after",
        deployment_id=deployment_id,
    )

    @task(task_id="example_code_python")
    def deployment_model_check_example(deployment_model_before, deployment_model_after):
        """Example of custom logic based on comparing old and replaced from the deployment."""

        # Put your custom logic here:
        return deployment_model_before["id"] != deployment_model_after["id"]

    example_deployment_model_check = deployment_model_check_example(
        deployment_model_before=get_deployment_model_before_op.output,
        deployment_model_after=get_deployment_model_after_op.output,
    )

    (
        get_deployment_model_before_op
        >> replace_deployment_model_op
        >> get_deployment_model_after_op
        >> example_deployment_model_check
    )


deployment_replace_model_dag = deployment_replace_model()

if __name__ == "__main__":
    deployment_replace_model_dag.test()
