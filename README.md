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
Currently supported types of credentials:

* `DataRobot Basic Credentials` - to store login/password pairs
* `DataRobot GCP Credentials` - to store Google Cloud Service account key
* `DataRobot AWS Credentials` - to store AWS access keys
* `DataRobot Azure Storage Credentials` - to store Azure Storage secret
* `DataRobot OAuth Credentials` - to store OAuth tokens
* `DataRobot JDBC DataSource` - to store JDBC connection attributes

After creating preconfigured connections using Airflow UI or Airflow API [Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html),
it can be used with `GetOrCreateCredentialOperator` or `GetOrCreateDataStoreOperator`
to replicate it in DataRobot and retrieve the corresponding `credentials_id`
or `datastore_id`.

## Config JSON for DAG run

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

### [Operators](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/operators/)

- `GetOrCreateCredentialOperator`

    Fetching credential by name and return `credential_id`. 
    Attempt to find DataRobot credential with the provided name and in case if credential does not exist it will create it using Airflow preconfigured connection
    with the same connection name.
 
    Required config params:

        `credentials_param_name`: str - name of parameter in the config file to get Credential name

    Returns a credential ID.

- `GetOrCreateDataStoreOperator`

    Fetching DataStore by Connection name and return DataStore ID. 
    In case if DataStore does not exist it will try to create it using Airflow preconfigured connection
    with the same connection name.
 
    Required config params:

        `connection_param_name`: str - name of parameter in the config file to get Connection name

    Returns a credential ID.

- `CreateDatasetFromDataStoreOperator`

    Loading dataset from JDBC Connection to DataRobot AI Catalog and return Dataset ID
 
    Required config params:

        - `datarobot_jdbc_connection`: str - existing of preconfigured Datarobot Connection name
        - `dataset_name`: str - name of loaded dataset
        - `table_schema`: str - database table schema
        - `table_name`: str - source table name
        - `do_snapshot`: bool - If unset, uses the server default(`True`). If `True`, creates a snapshot dataset; if `False`, creates a remote dataset. Creating snapshots from non-file sources may be disabled by the permission, _Disable AI Catalog Snapshots_.
        - `persist_data_after_ingestion`: bool - If unset, uses the server default(`True`). If `True`, will enforce saving all data (for download and sampling) and will allow a user to view extended data profile (which includes data statistics like min/max/median/mean, histogram, etc.). If `False`, will not enforce saving data. The data schema (feature names and types) still will be available. Specifying this parameter to **false** and `doSnapshot` to **true** will result in an error.

    Returns a Dataset ID.

- `UploadDatasetOperator`

    Uploading local file to DataRobot AI Catalog and return Dataset ID.
 
    Required config params:

        dataset_file_path: str - local path to training dataset

    Returns a Dataset ID.

- `UpdateDatasetFromFileOperator`

    Operator that creates a new Dataset version from a file.
    Returns when the new dataset version has been successfully uploaded.
 
    Required config params:

        dataset_id: str - DataRobot AI Catalog dataset ID
        dataset_file_path: str - local path to the training dataset

    Returns a Dataset version ID.

- `CreateDatasetVersionOperator`

    Creates a new version of the existing dataset in the AI Catalog and returns the dataset version ID.
 
    Required config params:

        dataset_id: str - DataRobot AI Catalog dataset ID
        datasource_id: str - existing DataRobot datasource ID
        credential_id: str - existing DataRobot credential ID

    Returns a Dataset version ID.

- `CreateOrUpdateDataSourceOperator`

    Creates the data source or updates it if its already exist and return DataSource ID.
 
    Required config params:

        data_store_id: str - DataRobot data store ID

    Returns a DataRobot DataSource ID.

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
    - You can use `GetOrCreateCredentialOperator` to pass `credential_id` from preconfigured DataRobot Credentials (Airflow Connections)
      or you can manually set `credential_id` parameter in the config. [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
    - OR a Dataset ID in the AI Catalog
    - OR a DataStore ID for jdbc source connection, you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from preconfigured Airflow Connection

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

- `GetServiceStatsOperator`

    Gets service stats measurements from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required. [Optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_service_stats) may be passed in the config as follows:

        "service_stats": {
            ...
        }

    Returns a dict with the service stats measurements data.

- `GetAccuracyOperator`

    Gets the accuracy of a deployment’s predictions.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required. [Optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_accuracy) may be passed in the config as follows:

        "accuracy": {
            ...
        }

    Returns a dict with the accuracy for a Deployment.

- `GetBiasAndFairnessSettingsOperator`

    Get Bias And Fairness settings for deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required.

    Returns a dict with the Bias And Fairness settings for a Deployment. More details: [get_bias_and_fairness_settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_bias_and_fairness_settings)
 
- `UpdateBiasAndFairnessSettingsOperator`

    Update Bias And Fairness settings for deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    Sample config params:

        "protected_features": ['attribute1'],
        "preferable_target_value": 'True',
        "fairness_metrics_set": 'equalParity',
        "fairness_threshold": 0.1,

- `GetSegmentAnalysisSettingsOperator`

    Get segment analysis settings for a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required.

    Returns a dict with segment analysis settings for a Deployment. More details: [get_segment_analysis_settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_segment_analysis_settings)

- `UpdateSegmentAnalysisSettingsOperator`

    Updates segment analysis settings for a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    Sample config params:

        "segment_analysis_enabled": True,
        "segment_analysis_attributes": ['attribute1', 'attribute2'],

- `GetMonitoringSettingsOperator`

    Get monitoring settings for deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    No config params are required.

    Returns a dict with the config params for a Deployment as follows:

        {
            "drift_tracking_settings": { ... } 
            "association_id_settings": { ... }
            "predictions_data_collection_settings": { ... }
        }

    where: - drift_tracking_settings: [drift tracking settings for this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_drift_tracking_settings)
           - association_id_settings: [association ID setting for this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_association_id_settings)
           - predictions_data_collection_settings: [predictions data collection settings of this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=predictions_data_collection_settings#datarobot.models.Deployment.get_predictions_data_collection_settings)
           
- `UpdateMonitoringSettingsOperator`

    Updates monitoring settings for a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID

    Sample config params:

        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "association_id_column": ["id"],
        "required_association_id": False,
        "predictions_data_collection_enabled": False,

- `BatchMonitoringOperator`

    Creates a batch monitoring job for the deployment.

    Prerequisites:
    - You can use `GetOrCreateCredentialOperator` to pass `credential_id` from preconfigured DataRobot Credentials (Airflow Connections)
      or you can manually set `credential_id` parameter in the config. [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
    - OR a Dataset ID in the AI Catalog
    - OR a DataStore ID for jdbc source connection, you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from preconfigured Airflow Connection

    Parameters:
  
        deployment_id: str - DataRobot Deployment ID
        datastore_id: str - DataRobot DataStore ID
        credential_id: str - DataRobot Credentials ID

    Sample config params:

        "deployment_id": "61150a2fadb5586af4118980",
        "monitoring_settings": {
            "intake_settings": {
                "type": "bigquery",
                "dataset": "integration_example_demo",
                "table": "actuals_demo",
                "bucket": "datarobot_demo_airflow",
            },
            "monitoring_columns": {
                "predictions_columns": [
                    {"class_name": "True", "column_name": "target_True_PREDICTION"},
                    {"class_name": "False", "column_name": "target_False_PREDICTION"},
                ],
                "association_id_column": "id",
                "actuals_value_column": "ACTUAL",
            },
        }

    Sample config params in case of manually set `credential_id` parameter in the config:

        "deployment_id": "61150a2fadb5586af4118980",
        "monitoring_settings": {
            "intake_settings": {
                "type": "bigquery",
                "dataset": "integration_example_demo",
                "table": "actuals_demo",
                "bucket": "datarobot_demo_airflow",
                "credential_id": "63eb7dfce1274472579f6e1c"
            },
            "monitoring_columns": {
                "predictions_columns": [
                    {"class_name": "True", "column_name": "target_True_PREDICTION"},
                    {"class_name": "False", "column_name": "target_False_PREDICTION"},
                ],
                "association_id_column": "id",
                "actuals_value_column": "ACTUAL",
            },
        }
    
    For more details: [batch monitoring settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.BatchMonitoringJob.run) see the DataRobot docs.

    Returns a batch monitoring job ID.

- `DownloadModelScoringCodeOperator`

    Downloads scoring code artifact from a Model.

    Parameters:

        project_id: str - DataRobot Project ID
        model_id: str - DataRobot Model ID
        base_path: str - base path for storing a downloaded model artifact

    Sample config params: [download scoring code parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=scoring_code#datarobot.models.Model.download_scoring_code) see the DataRobot docs.

        "source_code": False,

- `DownloadDeploymentScoringCodeOperator`

    Downloads scoring code artifact from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID
        base_path: str - base path for storing a downloaded model artifact

    Sample config params: [download scoring code parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=scoring_code#datarobot.models.Deployment.download_scoring_code) see the DataRobot docs.

        "source_code": False,
        "include_agent": False,
        "include_prediction_explanations": False,
        "include_prediction_intervals": False,

- `DownloadDeploymentScoringCodeOperator`

    Downloads scoring code artifact from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID
        base_path: str - base path for storing a downloaded model artifact

    Sample config params:

        "source_code": False,
        "include_agent": False,
        "include_prediction_explanations": False,
        "include_prediction_intervals": False,

- `SubmitActualsFromCatalogOperator`

    Downloads scoring code artifact from a deployment.

    Parameters:

        deployment_id: str - DataRobot deployment ID
        dataset_id: str - DataRobot Catalog dataset ID
        dataset_version_id: str - DataRobot Catalog dataset version ID

    Sample config params:

        "association_id_column": 'id',
        "actual_value_column": 'ACTUAL',
        "timestamp_column": 'timestamp',
        "was_acted_on_column": 'acted_on',

    Returns a uploading actuals job ID.

### [Sensors](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/sensors/datarobot.py)

- `AutopilotCompleteSensor`

    Checks whether the Autopilot has completed.

    Parameters:

        project_id: str - DataRobot project ID

- `ScoringCompleteSensor`

    Checks whether batch scoring has completed.

    Parameters:

        job_id: str - Batch prediction job ID

- `MonitoringJobCompleteSensor`

    Checks whether monitoring job is complete.

    Parameters:

        job_id: str - Batch Monitoring job ID

- `BaseAsyncResolutionSensor`

    Checks if the DataRobot Async API call has completed.

    Parameters:

        job_id: str - DataRobot async API call status check ID

### [Hooks](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/hooks/datarobot.py)

- `DataRobotHook`

    A hook for initializing DataRobot Public API client.


## Pipeline

The modules described above allows to construct a standard DataRobot pipeline in an Airflow dag:

    create_project_op >> train_models_op >> autopilot_complete_sensor >> deploy_model_op >> score_predictions_op >> scoring_complete_sensor


## Examples

See the [**examples**](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/example_dags) directory for the example DAGs.

Examples of using preconfigured connection you can find
in "datarobot_provider/example_dags" directory:

* `datarobot_pipeline_dag.py` -  example of Airflow DAG for the basic end-to-end workflow in DataRobot.
* `datarobot_score_dag.py` -  example of Airflow DAG for DataRobot batch scoring.
* `datarobot_jdbc_batch_scoring_dag.py` -  example of Airflow DAG for Batch Scoring with a JDBC data source.
* `datarobot_aws_s3_batch_scoring_dag.py` - example of Airflow DAG for using DataRobot AWS Credentials with ScorePredictionsOperator
* `datarobot_gcp_storage_batch_scoring_dag.py` - example of Airflow DAG for using DataRobot GCP Credentials with ScorePredictionsOperator
* `datarobot_bigquery_batch_scoring_dag.py` - example of Airflow DAG for using DataRobot GCP Credentials with ScorePredictionsOperator
* `datarobot_azure_storage_batch_scoring_dag.py` - example of Airflow DAG for using DataRobot Azure Storage Credentials with ScorePredictionsOperator
* `datarobot_jdbc_dataset_dag.py` - example of using a DataRobot JDBC connection to upload a dataset to the AI Catalog
* `datarobot_batch_monitoring_job_dag.py` - example of Airflow DAG to run a batch monitoring job
* `datarobot_create_project_from_ai_catalog_dag.py` - example of Airflow DAG for creating a DataRobot project from an AI Catalog dataset
* `datarobot_create_project_from_dataset_version_dag.py` -  example of Airflow DAG for creating a DataRobot project from a specific dataset version in the AI Catalog
* `datarobot_dataset_new_version_dag.py` -  example of creating new version of an existing dataset in AI Catalog
* `datarobot_dataset_upload_dag.py` -  example of Airflow DAG for uploading a local file to the DataRobot AI Catalog
* `datarobot_get_datastore_dag.py` -  example of Airflow DAG with GetOrCreateDataStoreOperator to create a Datarobot DataStore
* `datarobot_jdbc_dataset_dag.py` -  example of Airflow DAG for creating a DataRobot project from a JDBC data source
* `datarobot_jdbc_dynamic_dataset_dag.py` -  example of Airflow DAG for creating a DataRobot project from a JDBC dynamic data source
* `datarobot_upload_actuals_catalog_dag.py` -  example of Airflow DAG for uploading actuals from the AI Catalog
* `deployment_service_stats_dag.py` -  example of Airflow DAG for getting a deployment's service statistics with GetServiceStatsOperator
* `deployment_stat_and_accuracy_dag.py` -  example of Airflow DAG for getting a deployment's service statistics and accuracy
* `deployment_update_monitoring_settings_dag.py` -  example of Airflow DAG for updating a deployment's monitoring settings
* `deployment_update_segment_analysis_settings_dag.py` -  example of Airflow DAG for updating a deployment's segment analysis settings
* `download_scoring_code_from_deployment_dag.py` -  example of Airflow DAG for downloading scoring code (jar file) from a DataRobot deployment

## Issues

Please submit [issues](https://github.com/datarobot/airflow-provider-datarobot/issues) and [pull requests](https://github.com/datarobot/airflow-provider-datarobot/pulls) in our official repo:
[https://github.com/datarobot/airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot)

We are happy to hear from you. Please email any feedback to the authors at [support@datarobot.com](mailto:support@datarobot.com).


# Copyright Notice

Copyright 2023 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
