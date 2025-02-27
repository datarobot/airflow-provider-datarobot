# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from airflow.decorators import dag

from datarobot_provider.operators.deployment import DeployRegisteredModelOperator

"""
Example of Aiflow DAG for DataRobot data deployment and prediction generation.
Configurable parameters for this dag:
* model_package_id - The ID of the DataRobot model package (version) to deploy.
* deployment_label - A human readable label of the deployment.
* default_prediction_server_id - an identifier of a prediction server to be used as the default prediction server
  When working with prediction environments, default prediction server Id should not be provided
"""


@dag(
    schedule=None,
    tags=["example", "csv", "predictions", "deployment"],
    params={
        "model_package_id": "",
        "deployment_label": "hospital-readmissions-example-deployment-prediction",
        "default_prediction_server_id": "",
    },
)
def hospital_readmissions_deployment_prediction_generation():
    deploy_registered_model = DeployRegisteredModelOperator(
        task_id="deploy_registered_model",
        model_package_id="{{ params.model_package_id }}",
        deployment_label="{{ params.deployment_label }}",
        extra_params={"default_prediction_server_id": "{{ params.default_prediction_server_id }}"},
    )

    deploy_registered_model


hospital_readmissions_deployment_prediction_generation()
