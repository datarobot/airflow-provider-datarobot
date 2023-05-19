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


## Creating DataRobot preconfigured Connections

You can create preconfigured Connections to store and manage credentials 
that can be used together with Airflow Operators to replicate connection 
on DataRobot side ([Data connections](https://docs.datarobot.com/en/docs/data/connect-data/stored-creds.html)).
Currently, supported the next types of credentials:

* `DataRobot Basic Credentials` - to store login/password pairs
* `DataRobot GCP Credentials` - to store Google Cloud Service account key
* `DataRobot AWS Credentials` - to store AWS access keys
* `DataRobot Azure Storage Credentials` - to store Azure Storage secret
* `DataRobot OAuth Credentials` - to store OAuth tokens
* `DataRobot JDBC DataSource` - to store JDBC connection attributes

After creating preconfigured Connections using Airflow UI or Airflow API [Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
it can be used with GetCredentialIdOperator or GetOrCreateDataStoreOperator
to replicate it on DataRobot side and retrieve corresponding credentials_id
or datastore_id. 
Examples of using preconfigured connection you can find
in "datarobot_provider/example_dags" directory:

* `datarobot_aws_s3_batch_scoring_dag.py` - example of using DataRobot AWS Credentials with ScorePredictionsOperator
* `datarobot_azure_storage_batch_scoring_dag.py` - example of using DataRobot GCP Credentials with ScorePredictionsOperator
* `datarobot_azure_storage_batch_scoring_dag.py` - example of using DataRobot Azure Storage Credentials with ScorePredictionsOperator
* `datarobot_jdbc_dataset_dag.py` - example of using DataRobot JDBC Connection to upload dataset to AI Catalog

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

- `UploadDatasetOperator`

    Uploading local file to DataRobot AI Catalog and return Dataset ID.
 
    Required config params:

        dataset_file_path: str - local path to training dataset

    Returns a dataset ID.

- `CreateProjectOperator`

    Creates a DataRobot project and returns its ID.
 
    Several options of source dataset supported:
  
   - Creating project directly from local file or pre-signed S3 URL. Required config params:

          training_data: str - pre-signed S3 URL or local path to training dataset
          project_name: str - project name

      In case of an S3 input, the `training_data` value must be a [pre-signed AWS S3 URL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

      For more [project settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#project) see the DataRobot docs.

      Returns a project ID.
  
   - Creating project from existing dataset in AI Catalog, using dataset_id from config file. Required config params:

          training_dataset_id: str - dataset_id corresponding to existing dataset in DataRobot AICatalog
          project_name: str - project name

      For more [project settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#project) see the DataRobot docs.

      Returns a project ID.

   - Creating project from an existing dataset in DataRobot AI Catalog, using dataset_id coming from previous operator. 
     In this case your previous operator must return valid dataset_id (for example `UploadDatasetOperator`) and you 
     should use this output value as a 'dataset_id' argument in `CreateProjectOperator` object creation step. 
     Required config params:

          project_name: str - project name
  
      For more [project settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#project) see the DataRobot docs.

      Returns a project ID.

- `TrainModelsOperator`

    Triggers DataRobot Autopilot to train models.

    Parameters:

        project_id: str - DataRobot project ID

    Required config params:

        "autopilot_settings": {
            "target": "readmitted"
        } 
         
    `target` is a required parameter with the column name which defines the modeling target.
    
    For more [autopilot settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Project.set_target) see the DataRobot docs.

    Returns `None`.

- `DeployModelOperator`

    Deploys a specified model.

    Parameters:

        model_id: str - DataRobot model ID

    Required config params:

        deployment_label - deployment label/name

    For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment) see the DataRobot docs.

    Returns a deployment ID.

- `DeployRecommendedModelOperator`

    Deploys a recommended model.

    Parameters:

        project_id: str - DataRobot project ID

    Required config params:

        deployment_label: str - deployment label

    For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment) see the DataRobot docs.

    Returns a deployment ID.

- `ScorePredictionsOperator`

    Scores batch predictions against the deployment.

    Prerequisites:
    - You can use GetCredentialIdOperator to pass `credential_id` from preconfigured DataRobot Credentials (Airflow Connections)
      or you can manually set `credential_id` parameter in the config. [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
    - OR a Dataset ID in the AI Catalog
    - OR a DataStore ID for jdbc source connection

    Parameters:
  
        deployment_id: str - DataRobot deployment ID
        intake_datastore_id: str - DataRobot DataStore ID for jdbc source connection
        output_datastore_id: str - DataRobot DataStore ID for jdbc destination connection
        intake_credential_id: str - DataRobot Credentials ID for source connection
        output_credential_id: str - DataRobot Credentials ID for destination connection

    Sample config params:

        "score_settings": {
            "intake_settings": {
                "type": "s3",
                "url": "s3://my-bucket/Diabetes10k.csv",
            },
            "output_settings": {
                "type": "s3",
                "url": "s3://my-bucket/Diabetes10k_predictions.csv",
            }
        }

    Sample config params in case of manually set `credential_id` parameter in the config:

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
                "dataset_id": "<datasetId>",
            },
            "output_settings": {
                ...
            }
        }
    
    For more [batch prediction settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.BatchPredictionJob.score) see the DataRobot docs.

    Returns a batch prediction job ID.

- `GetTargetDriftOperator`

    Gets the target drift from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required. [Optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_target_drift) may be passed in the config as follows:

        "target_drift": {
            ...
        }

    Returns a dict with the target drift data.

- `GetFeatureDriftOperator`

    Gets the feature drift from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required. [Optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_feature_drift) may be passed in the config as follows:

        "feature_drift": {
            ...
        }

    Returns a dict with the feature drift data.

### [Sensors](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/sensors/datarobot.py)

- `AutopilotCompleteSensor`

    Checks whether the Autopilot has completed.

    Parameters:

        project_id: str - DataRobot project ID

- `ScoringCompleteSensor`

    Checks whether batch scoring has completed.

    Parameters:

        job_id: str - Batch prediction job ID

### [Hooks](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/hooks/datarobot.py)

- `DataRobotHook`

    A hook for initializing DataRobot Public API client.


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

Copyright 2023 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
