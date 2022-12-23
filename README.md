# DataRobot Provider for Apache Airflow

This package provides operators, sensors, and a hook that integrates [DataRobot](https://www.datarobot.com) into Apache Airflow.
Using these components you should be able to build the essential DataRobot pipeline - create a project, train models, deploy a model,
score predictions against the model deployment.


## Installation

Prerequisites:
- [Apache Airflow](https://pypi.org/project/apache-airflow/)
- [DataRobot Python API client](https://pypi.org/project/datarobot/)

Install the DataRobot provider:
```
pip install airflow-provider-datarobot
```


## Connection

In the Airflow user interface, create a new DataRobot connection in `Admin > Connections`:

* Connection Type: `DataRobot`
* Connection Id: `datarobot_default` (default)
* API Key: `your-datarobot-api-key`
* DataRobot endpoint URL: `https://app.datarobot.com/api/v2` (default)

Create the API Key in the [DataRobot Developer Tools](https://app.datarobot.com/account/developer-tools) page, `API Keys` section (see [DataRobot Docs](https://app.datarobot.com/docs/api/api-quickstart/api-qs.html#create-a-datarobot-api-token) for more details).

By default, all components use `datarobot_default` connection ID.


## Config JSON for dag run

Operators and sensors use parameters from the [config](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html?highlight=config#Named%20Arguments_repeat21)
which must be submitted when triggering the dag. Example config JSON with required parameters:

    {
        "training_data": "s3-presigned-url-or-local-path-to-training-data",
        "project_name": "Project created from Airflow",
        "autopilot_settings": {
            "target": "readmitted"
        },
        "deployment_label": "Deployment created from Airflow",
        "score_settings": {
            "intake_settings": {
                "type": "s3",
                "url": "s3://path/to/scoring-data/Diabetes10k.csv",
                "credential_id": "62160b511fb29da8dd5f2c81"
            },
            "output_settings": {
                "type": "s3",
                "url": "s3://path/to/results-dir/Diabetes10k_predictions.csv",
                "credential_id": "62160b511fb29da8dd5f2c81"
            }
        }
    }
    

These config values can be accessed in the `execute()` method of any operator the dag 
in the `context["params"]` variable, e.g. getting a training data you would use this in the operator:

    def execute(self, context: Dict[str, Any]) -> str:
        ...
        training_data = context["params"]["training_data"]
        ...


## Modules

### [Operators](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/operators/datarobot.py)

- `CreateProjectOperator` - creates a DataRobot project and returns its ID
 
    Required config params:

        training_data: str - pre-signed S3 URL or local path to training dataset
        project_name: str - project name

    In case of an S3 input, the `training_data` value must be a [pre-signed AWS S3 URL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

    For more [project settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/autodoc/api_reference.html#project) see the DataRobot docs.

- `TrainModelsOperator` - triggers DataRobot Autopilot to train models

    Parameters:

        project_id: str - DataRobot project ID

    Required config params:

        "autopilot_settings": {
            "target": "readmitted"
        } 
         
    `target` is a required parameter with the column name which defines the modeling target.
    
    For more [autopilot settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/autodoc/api_reference.html#datarobot.models.Project.set_target) see the DataRobot docs.

- `DeployModelOperator` - deploys a specified model and returns the deployment ID

    Parameters:

        model_id: str - DataRobot model ID

    Required config params:

        deployment_label - deployment label/name

    For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/autodoc/api_reference.html#deployment) see the DataRobot docs.

- `DeployRecommendedModelOperator` - deploys a recommended model and returns the deployment ID

    Parameters:

        project_id: str - DataRobot project ID

    Required config params:

        deployment_label: str - deployment label

    For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/autodoc/api_reference.html#deployment) see the DataRobot docs.

- `ScorePredictionsOperator` - scores predictions against the deployment and returns a batch prediction job ID

    Prerequisites:
    - [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/reference/admin/credentials.html#s3-credentials).
      You need the `creds.credential_id` for the `credential_id` parameter in the config.
    - OR the ID of a Dataset in the AI Catalog

    Parameters:
  
        deployment_id: str - DataRobot project ID

    Required config params:

        "score_settings": {
            "intake_settings": {
                "type": "s3",
                "url": "s3://my-bucket/Diabetes10k.csv",
                "credential_id": "62160b511fb29da8dd5f2c81"
            },
            "output_settings": {
                "type": "s3",
                "url": "s3://my-bucket/Diabetes10k_predictions.csv",
                "credential_id": "62160b511fb29da8dd5f2c81"
            }
        }

    Config params for scoring a Dataset in the AI Catalog:

        "score_settings": {
            "intake_settings": {
                "type": "dataset",
                "datasetId": "<datasetid>",
            },
            "output_settings": {
            ...
            }
        }
    
    For more [batch prediction settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/v2.27.1/autodoc/api_reference.html#batch-predictions) see the DataRobot docs.

### [Sensors](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/sensors/datarobot.py)

- `AutopilotCompleteSensor` - checks whether the Autopilot has completed

    Parameters:

        project_id: str - DataRobot project ID

- `ScoringCompleteSensor` - checks whether batch scoring has completed

    Parameters:

        job_id: str - Batch prediction job ID

### [Hooks](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/hooks/datarobot.py)

- `DataRobotHook` - a hook for initializing DataRobot Public API client


## Pipeline

The modules described above allows to construct a standard DataRobot pipeline in an Airflow dag:

    create_project_op >> train_models_op >> autopilot_complete_sensor >> deploy_model_op >> score_predictions_op >> scoring_complete_sensor


## Examples

See the [**examples**](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/example_dags) directory for the example DAGs.


## Issues

Please submit [issues](https://github.com/datarobot/airflow-provider-datarobot/issues) and [pull requests](https://github.com/datarobot/airflow-provider-datarobot/pulls) in our official repo:
[https://github.com/datarobot/airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot)

We are happy to hear from you. Please email any feedback to the authors at [support@datarobot.com](mailto:support@datarobot.com).


# Copyright Notice

Copyright 2022 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
