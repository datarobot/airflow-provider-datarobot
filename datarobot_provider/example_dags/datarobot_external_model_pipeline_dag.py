# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datetime import datetime

from airflow.decorators import dag
from datarobot import TARGET_TYPE
from datarobot.enums import DEPLOYMENT_IMPORTANCE

from datarobot_provider.operators.model_package import CreateExternalModelPackageOperator
from datarobot_provider.operators.model_package import DeployModelPackageOperator
from datarobot_provider.operators.monitoring import UpdateMonitoringSettingsOperator

"""
Example of JSON configuration for a regression model:

.. code-block:: python

    {
         "name": "Lending club regression",
         "modelDescription": {
                 "description": "Regression on lending club dataset"
             }
         "target": {
             "type": "Regression",
             "name": "loan_amnt"
         }
    }


Example JSON for a binary classification model:

.. code-block:: python

    {
        "name": "Surgical Model",
        "modelDescription": {
            "description": "Binary classification on surgical dataset",
            "location": "/tmp/myModel"
            },
            "target": {
                "type": "Binary",
                "name": "complication",
                "classNames": ["Yes","No"],  # minority/positive class should be listed first
                "predictionThreshold": 0.5
            }
        }
    }

Example JSON for a multiclass classification model:

.. code-block:: python

    {
        "name": "Iris classifier",
        "modelDescription": {
        "description": "Classification on iris dataset",
        "location": "/tmp/myModel"
    },
        "target": {
            "type": "Multiclass",
            "name": "Species",
            "classNames": [
                "Iris-versicolor",
                "Iris-virginica",
                "Iris-setosa"
            ]
        }
    }
"""


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops", "external model"],
    params={
        "model_package_json": {
            "name": "Demo Regression Model",
            "modelDescription": {"description": "Regression on demo dataset"},
            "target": {"type": TARGET_TYPE.REGRESSION, "name": "Grade 2014"},
        },
        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "association_id_column": ["id"],
        "required_association_id": False,
        "predictions_data_collection_enabled": False,
    },
)
def create_external_deployment_pipeline(prediction_environment_id=None):
    if not prediction_environment_id:
        raise ValueError("Invalid or missing `prediction_environment_id` value")

    create_model_package_op = CreateExternalModelPackageOperator(
        task_id="create_model_package",
    )

    deploy_model_package_op = DeployModelPackageOperator(
        task_id="deployment_from_model_package",
        deployment_name="demo_airflow_deployment",
        description="demo_airflow_deployment",
        model_package_id=create_model_package_op.output,
        prediction_environment_id=prediction_environment_id,
        importance=DEPLOYMENT_IMPORTANCE.LOW,
    )

    update_monitoring_settings_op = UpdateMonitoringSettingsOperator(
        task_id="update_monitoring_settings",
        deployment_id=deploy_model_package_op.output,
    )

    create_model_package_op >> deploy_model_package_op >> update_monitoring_settings_op


create_external_deployment_pipeline_dag = create_external_deployment_pipeline()

if __name__ == "__main__":
    create_external_deployment_pipeline_dag.test()
