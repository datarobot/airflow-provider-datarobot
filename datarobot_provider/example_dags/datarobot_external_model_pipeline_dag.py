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

from datarobot_provider.operators.model_package import CreateExternalModelPackageOperator

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
    tags=['example', 'mlops', 'external model'],
    params={
        "model_package_json": {
            "name": "Demo Regression Model",
            "modelDescription": {"description": "Regression on demo dataset"},
            "target": {"type": TARGET_TYPE.REGRESSION, "name": 'Grade 2014'},
        },
    },
)
def create_external_deployment_pipeline():
    create_model_package_op = CreateExternalModelPackageOperator(
        task_id='create_model_package',
    )

    create_model_package_op


create_external_deployment_pipeline_dag = create_external_deployment_pipeline()

if __name__ == "__main__":
    create_external_deployment_pipeline_dag.test()
