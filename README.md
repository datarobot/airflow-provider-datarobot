# DataRobot Provider for Apache Airflow

This package provides operators, sensors, and a hook to integrate [DataRobot](https://www.datarobot.com) into Apache Airflow.
Using these components, you should be able to build the essential DataRobot pipeline - create a project, train models, deploy a model,
and score predictions against the model deployment.

## Install the Airflow provider

The DataRobot provider for Apache Airflow requires an environment with the following dependencies installed:

* [Apache Airflow](https://pypi.org/project/apache-airflow/) >= 2.3

* [DataRobot Python API Client](https://pypi.org/project/datarobot/) >= 3.2.0

To install the DataRobot provider, run the following command:

``` sh
pip install airflow-provider-datarobot
```

## Create a connection from Airflow to DataRobot

The next step is to create a connection from Airflow to DataRobot:

1. In the Airflow user interface, click **Admin > Connections** to 
   [add an Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui).

2. On the **List Connection** page, click **+ Add a new record**.

3. In the **Add Connection** dialog box, configure the following fields:

    | Field          | Description |
    |----------------|-------------|
    |Connection Id   | `datarobot_default` (this name is used by default in all operators) |
    |Connection Type | DataRobot |
    |API Key         | A DataRobot API key, created in the [DataRobot Developer Tools](https://app.datarobot.com/account/developer-tools), from the [*API Keys* section](https://app.datarobot.com/docs/api/api-quickstart/api-qs.html#create-a-datarobot-api-token). |
    |DataRobot endpoint URL | `https://app.datarobot.com/api/v2` by default |

4. Click **Test** to establish a test connection between Airflow and DataRobot.

5. When the connection test is successful, click **Save**.

## Create preconfigured connections to DataRobot

You can create preconfigured connections to store and manage credentials to use with Airflow Operators, 
replicating the [connection on the DataRobot side](https://docs.datarobot.com/en/docs/data/connect-data/stored-creds.html).

Currently, the supported credential types are:

| Credentials                           | Description          |
|---------------------------------------|----------------------|
| `DataRobot Basic Credentials`         | Login/password pairs |
| `DataRobot GCP Credentials`           | Google Cloud Service account key |
| `DataRobot AWS Credentials`           | AWS access keys      |
| `DataRobot Azure Storage Credentials` | Azure Storage secret |
| `DataRobot OAuth Credentials`         | OAuth tokens         |
| `DataRobot JDBC DataSource`           | JDBC connection attributes |

After [creating a preconfigured connection through the Airflow UI or API](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html),
you can access your stored credentials with `GetOrCreateCredentialOperator` or `GetOrCreateDataStoreOperator`
to replicate them in DataRobot and retrieve the corresponding `credentials_id` or `datastore_id`.

## JSON configuration for the DAG run

Operators and sensors use parameters from the [config](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html?highlight=config#Named%20Arguments_repeat21) JSON submitted when triggering the DAG; for example:


``` yaml
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
            "credential_id": "<credential_id>"
        },
        "output_settings": {
            "type": "s3",
            "url": "s3://path/to/results-dir/Diabetes10k_predictions.csv",
            "credential_id": "<credential_id>"
        }
    }
}
```
    

These config values are accessible in the `execute()` method of any operator in the DAG 
through the `context["params"]` variable; for example, to get training data, you could use the following:

``` py
def execute(self, context: Dict[str, Any]) -> str:
    ...
    training_data = context["params"]["training_data"]
    ...
```


## Modules

### [Operators](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/operators/)

#### `GetOrCreateCredentialOperator`

Fetches a credential by name. This operator attempts to find a DataRobot credential with the provided name. If the credential doesn't exist, the operator creates it using the Airflow preconfigured connection with the same connection name.

Returns a credential ID.

Required config parameters:

| Parameter                | Type | Description |
|--------------------------|------|-------------|
| `credentials_param_name` | str  | The name of parameter in the config file for the credential name. |

----

#### `GetOrCreateDataStoreOperator`

Fetches a DataStore by Connection name. If the DataStore does not exist, the operator attempts to create it using Airflow preconfigured connection with the same connection name.

Returns a credential ID.

Required config params:

| Parameter                | Type | Description |
|--------------------------|------|-------------|
| `connection_param_name`  | str  | The name of the parameter in the config file for the connection name. |

---

#### `CreateDatasetFromDataStoreOperator`

Loads a dataset from a JDBC Connection to the DataRobot AI Catalog.

Returns a dataset ID.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `datarobot_jdbc_connection` | str  | The existing preconfigured DataRobot JDBC connection name. |
| `dataset_name`              | str  | The name of the loaded dataset. |
| `table_schema`              | str  | The database table schema. |
| `table_name`                | str  | The source table name. |
| `do_snapshot`               | bool | If `True`, creates a snapshot dataset. If `False`, creates a remote dataset. If unset, uses the server default (`True`). Creating snapshots from non-file sources may be disabled by the _Disable AI Catalog Snapshots_ permission. |
| `persist_data_after_ingestion` | bool | If `True`, enforce saving all data (for download and sampling) and allow a user to view the extended data profile (which includes data statistics like min, max, median, mean, histogram, etc.). If `False`, don't enforce saving data. If unset, uses the server default (`True`). The data schema (feature names and types) will still be available. Specifying this parameter to `False` and `doSnapshot` to `True` results in an error. |

---

#### `UploadDatasetOperator`

Uploads a local file to the DataRobot AI Catalog.

Returns a dataset ID.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `dataset_file_path`         | str  | The local path to the training dataset. |

---

#### `UpdateDatasetFromFileOperator`

Creates a new dataset version from a file. 

Returns a dataset version ID when the new version uploads successfully.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `dataset_id`                | str  | The DataRobot AI Catalog dataset ID. |
| `dataset_file_path`         | str  | The local path to the training dataset. |

---

#### `CreateDatasetVersionOperator`

Creates a new version of the existing dataset in the AI Catalog.

Returns a dataset version ID.
 
Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `dataset_id`                | str  | The DataRobot AI Catalog dataset ID. |
| `datasource_id`             | str  | The existing DataRobot datasource ID. |
| `credential_id `            | str  | The existing DataRobot credential ID. |

---

#### `CreateOrUpdateDataSourceOperator`

Creates a data source or updates it if it already exists.

Returns a DataRobot DataSource ID.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `data_store_id`             | str  | THe DataRobot datastore ID. |

---

#### `CreateProjectOperator`

Creates a DataRobot project.

Returns a project ID.
 
Several options of source dataset supported:

<details>

<summary>Local file or pre-signed S3 URL</summary>

Create a project directly from a local file or a pre-signed S3 URL.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `training_data`             | str  | The pre-signed S3 URL or the local path to the training dataset. |
| `project_name`              | str  | The project name. |

> **Note:** In case of an S3 input, the `training_data` value must be a [pre-signed AWS S3 URL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

</details>
  
<details>

<summary>AI Catalog dataset from config file</summary>
  
Create a project from an existing dataset in the DataRobot AI Catalog using a dataset ID defined in the config file.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `training_dataset_id`       | str  | The dataset ID corresponding to existing dataset in the DataRobot AI Catalog. |
| `project_name`              | str  | The project name. |

</details>

<details>

<summary>AI Catalog dataset from previous operator</summary>

Create a project from an existing dataset in the DataRobot AI Catalog using a dataset ID from the previous operator. 
In this case, your previous operator must return a valid dataset ID (for example `UploadDatasetOperator`) 
and you should use this output value as a `dataset_id` argument in the `CreateProjectOperator` object creation step.

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `project_name`              | str  | The project name. |

</details>

For more [project settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#project), see the DataRobot documentation.

---

#### `TrainModelsOperator`

Runs DataRobot Autopilot to train models.

Returns `None`.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `project_id`                | str  | The DataRobot project ID. |

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `target`                    | str  | The name of the column defining the modeling target. |

``` json
"autopilot_settings": {
    "target": "readmitted"
} 
```

For more [autopilot settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Project.set_target), see the DataRobot documentation.

---

#### `DeployModelOperator`

Deploy a specified model.

Returns a deployment ID.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `model_id`                  | str  | The DataRobot model ID. |

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_label`          | str  | The deployment label name. |

For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment), see the DataRobot documentation.

---

#### `DeployRecommendedModelOperator`

Deploys a recommended model.

Returns a deployment ID.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `project_id`                | str  | The DataRobot project ID. |

Required config params:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
`deployment_label`            | str  | The deployment label name. |

For more [deployment settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment), see the DataRobot documentation.

---

#### `ScorePredictionsOperator`

Scores batch predictions against the deployment.

Returns a batch prediction job ID.

Prerequisites:

- Use `GetOrCreateCredentialOperator` to pass a `credential_id` from the preconfigured DataRobot Credentials (Airflow Connections) or manually set the `credential_id` parameter in the config.

    > **Note:** You can [add S3 credentials to DataRobot via the Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
    
- _Or_ use a Dataset ID from the DataRobot AI Catalog.
- _Or_ use a DataStore ID for a JDBC source connection; you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from a preconfigured Airflow Connection.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |
| `intake_datastore_id`       | str  | The DataRobot datastore ID for the JDBC source connection. |
| `output_datastore_id`       | str  | The DataRobot datastore ID for the JDBC destination connection. |
| `intake_credential_id`      | str  | The DataRobot credentials ID for the source connection. |
| `output_credential_id`      | str  | The DataRobot credentials ID for the destination connection. |

<details>

<summary>Sample config: Pre-signed S3 URL</summary>

``` json
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
```

</details>

<details>

<summary>Sample config: Pre-signed S3 URL with a manually set credential ID</summary>

``` json
"score_settings": {
    "intake_settings": {
        "type": "s3",
        "url": "s3://my-bucket/Diabetes10k.csv",
        "credential_id": "<credential_id>"
    },
    "output_settings": {
        "type": "s3",
        "url": "s3://my-bucket/Diabetes10k_predictions.csv",
        "credential_id": "<credential_id>"
    }
}
```

</details>

<details>

<summary>Sample config: Scoring dataset in the AI Catalog</summary>

``` json
"score_settings": {
    "intake_settings": {
        "type": "dataset",
        "dataset_id": "<datasetId>",
    },
    "output_settings": { }
}
```
</details>

For more [batch prediction settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.BatchPredictionJob.score), see the DataRobot documentation.

---

#### `GetTargetDriftOperator`

Gets the target drift from a deployment.

Returns a dict with the target drift data.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

No config params are required; however, the [optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_target_drift) may be passed in the config as follows:

``` json
"target_drift": { }
```

---

#### `GetFeatureDriftOperator`

Gets the feature drift from a deployment.

Returns a dict with the feature drift data.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

No config params are required; however, the [optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_feature_drift) may be passed in the config as follows:

``` json
"feature_drift": { }
```

---

#### `GetServiceStatsOperator`

Gets service stats measurements from a deployment.

Returns a dict with the service stats measurements data.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

No config params are required; however, the [optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_service_stats) may be passed in the config as follows:

``` json
"service_stats": { }
```

---

#### `GetAccuracyOperator`

Gets the accuracy of a deployment’s predictions.

Returns a dict with the accuracy for a deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

No config params are required; however, the [optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_accuracy) may be passed in the config as follows:

``` json
"accuracy": { }
```

---

#### `GetBiasAndFairnessSettingsOperator`

Gets the Bias And Fairness settings for deployment.

Returns a dict with the [Bias And Fairness settings for a Deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_bias_and_fairness_settings).

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

No config params are required.

---

#### `UpdateBiasAndFairnessSettingsOperator`

Updates the Bias And Fairness settings for deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

Sample config params:

```
"protected_features": ["attribute1"],
"preferable_target_value": "True",
"fairness_metrics_set": "equalParity",
"fairness_threshold": 0.1,
```

---

#### `GetSegmentAnalysisSettingsOperator`

Gets the segment analysis settings for a deployment.

Returns a dict with the [segment analysis settings for a deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_segment_analysis_settings)

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

No config params are required.

---

#### `UpdateSegmentAnalysisSettingsOperator`

Updates the segment analysis settings for a deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

Sample config params:

```
"segment_analysis_enabled": True,
"segment_analysis_attributes": ["attribute1", "attribute2"],
```

---

#### `GetMonitoringSettingsOperator`

Gets the monitoring settings for deployment.

Returns a dict with the config params for a deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

No config params are required.

Sample monitoring settings:

```
{
"drift_tracking_settings": {  } 
"association_id_settings": {  }
"predictions_data_collection_settings": {  }
}
```

| Dictionary | Description  |
|------------|--------------|
|`drift_tracking_settings`  | The [drift tracking settings for this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_drift_tracking_settings). |
| `association_id_settings` | The [association ID settings for this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_association_id_settings). |
| `predictions_data_collection_settings` | The [predictions data collection settings of this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=predictions_data_collection_settings#datarobot.models.Deployment.get_predictions_data_collection_settings). |

---
    
#### `UpdateMonitoringSettingsOperator`

Updates monitoring settings for a deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |

Sample config params:

```
"target_drift_enabled": True,
"feature_drift_enabled": True,
"association_id_column": ["id"],
"required_association_id": False,
"predictions_data_collection_enabled": False,
```

---

#### `BatchMonitoringOperator`

Creates a batch monitoring job for the deployment.

Returns a batch monitoring job ID.

Prerequisites:

- Use `GetOrCreateCredentialOperator` to pass a `credential_id` from the preconfigured DataRobot Credentials (Airflow Connections) or manually set the `credential_id` parameter in the config.

    > **Note:** You can [add S3 credentials to DataRobot via the Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).

- _Or_ use a Dataset ID from the DataRobot AI Catalog.
- _Or_ use a DataStore ID for a JDBC source connection; you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from a preconfigured Airflow Connection.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |
| `datastore_id`              | str  | The DataRobot datastore ID. |
| `credential_id`             | str  | The DataRobot credentials ID. |

Sample config params:

<details>

<summary>Sample config</summary>

``` json
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
```

</details>

<details>

<summary>Sample config: Manually set credential ID</summary>

``` json
"deployment_id": "61150a2fadb5586af4118980",
"monitoring_settings": {
    "intake_settings": {
        "type": "bigquery",
        "dataset": "integration_example_demo",
        "table": "actuals_demo",
        "bucket": "datarobot_demo_airflow",
        "credential_id": "<credential_id>"
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
```

</details>

For more [batch monitoring settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.BatchMonitoringJob.run), see the DataRobot documentation.

---

#### `DownloadModelScoringCodeOperator`

Downloads scoring code artifact from a model.

Parameters:

| Parameter                   | Type | Description                                            |
|-----------------------------|------|--------------------------------------------------------|
| `project_id`                | str  | The DataRobot project ID.                              |
| `model_id`                  | str  | The DataRobot model ID.                                |
| `base_path`                 | str  | The base path for storing a downloaded model artifact. |

Sample config params: 

```
"source_code": False,
```

For more [scoring code download parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=scoring_code#datarobot.models.Model.download_scoring_code), see the DataRobot documentation.

---

#### `DownloadDeploymentScoringCodeOperator`

Downloads scoring code artifact from a deployment.

Parameters:

| Parameter                   | Type | Description                                            |
|-----------------------------|------|--------------------------------------------------------|
| `deployment_id`             | str  | The DataRobot deployment ID.                           |
| `base_path`                 | str  | The base path for storing a downloaded model artifact. |

Sample config params:

```
"source_code": False,
"include_agent": False,
"include_prediction_explanations": False,
"include_prediction_intervals": False,
```

For more [scoring code download parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=scoring_code#datarobot.models.Deployment.download_scoring_code), see the DataRobot documentation.

---

#### `SubmitActualsFromCatalogOperator`

Downloads scoring code artifact from a deployment.

Returns an actuals upload job ID.

Parameters:

| Parameter                   | Type | Description                                  |
|-----------------------------|------|----------------------------------------------|
| `deployment_id`             | str  | The DataRobot deployment ID.                 |
| `dataset_id`                | str  | The DataRobot AI Catalog dataset ID.         |
| `dataset_version_id`        | str  | The DataRobot AI Catalog dataset version ID. |

Sample config params:

```
"association_id_column": "id",
"actual_value_column": "ACTUAL",
"timestamp_column": "timestamp",
```

---

#### `StartAutopilotOperator`

Triggers DataRobot Autopilot to train a set of models.

Parameters:

| Parameter                        | Type | Description                                   |
|----------------------------------|------|-----------------------------------------------|
| `project_id`                     | str  | The DataRobot project ID.                     |
| `featurelist_id`                 | str  | Specifies which feature list to use.          |
| `relationships_configuration_id` | str  | ID of the relationships configuration to use. |
| `segmentation_task_id`           | str  | ID of the segementation task to use. |

Sample config params:

```
"autopilot_settings": {
    "target": "column_name",
    "mode": AUTOPILOT_MODE.QUICK,
}
```

For more [analyze_and_model parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Project.analyze_and_model), see the DataRobot documentation.

---

#### `CreateExecutionEnvironmentOperator`

Create an execution environment.

Returns an execution environment ID.

Parameters:

| Parameter              | Type | Description                                            |
|------------------------|------|--------------------------------------------------------|
| `name`                 | str  | The execution environment name.                        |
| `description`          | str  | The execution environment description.                     |
| `programming_language` | str  | The programming language of the environment to be created. |

Sample config params:

```
"execution_environment_name": "My Demo Env",
"custom_model_description": "This is a custom model created by Airflow",
"programming_language": "python",
```

For more [execution environment creation parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=executionenvironmentversion%20create#datarobot.ExecutionEnvironment.create), see the DataRobot documentation.

---

#### `CreateExecutionEnvironmentVersionOperator`

Create an execution environment version.

Returns a version ID for the newly created execution environment .

Parameters:

| Parameter                          | Type | Description                                       |
|------------------------------------|------|---------------------------------------------------|
| `execution_environment_id`         | str  | The ID of the execution environment.              |
| `docker_context_path`              | str  | The file path to a Docker context archive or folder.   |
| `environment_version_label`        | str  | A short, human-readable string to label the environment version. |
| `environment_version_description`  | str  | The execution environment version description.        |

For more [execution environment version creation parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/mlops/custom_model.html?highlight=ExecutionEnvironmentVersion#create-execution-environment), see the DataRobot documentation.

---

#### `CreateCustomInferenceModelOperator`

Create a custom inference model.

Returns the ID for the created custom model.

Parameters:

| Parameter          | Type | Description                           |
|--------------------|------|---------------------------------------|
| `name`             | str  | Name of the custom model.             |
| `description`      | str  | Description of the custom model.      |

Sample DAG config params:

```
"target_type": - Target type of the custom inference model.
    Values: [`datarobot.TARGET_TYPE.BINARY`, `datarobot.TARGET_TYPE.REGRESSION`,
    `datarobot.TARGET_TYPE.MULTICLASS`, `datarobot.TARGET_TYPE.UNSTRUCTURED`]
"target_name": - Target feature name.
    It is optional (ignored if provided) for `datarobot.TARGET_TYPE.UNSTRUCTURED` target type.
"programming_language": - Programming language of the custom learning model.
"positive_class_label": - Custom inference model positive class label for binary classification.
"negative_class_label": - Custom inference model negative class label for binary classification.
"prediction_threshold": - Custom inference model prediction threshold.
"class_labels": - Custom inference model class labels for multiclass classification.
"network_egress_policy": - Determines whether the given custom model is isolated, or can access the public network.
"maximum_memory": - The maximum memory that might be allocated by the custom model.
"replicas": - A fixed number of replicas that will be deployed in the cluster.
```

For more [custom inference model creation parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/mlops/custom_model.html?highlight=CustomInferenceModel#create-custom-inference-model), see the DataRobot documentation.

---

#### `CreateCustomModelVersionOperator`

Create a custom model version.

Returns the version ID for the created custom model.

Parameters:

| Parameter                      | Type | Description                                                                               |
|--------------------------------|------|-------------------------------------------------------------------------------------------|
| `custom_model_id`              | str  | The ID of the custom model.                                                               |
| `base_environment_id`          | str  | The ID of the base environment to use with the custom model version.                      |
| `training_dataset_id`          | str  | The ID of the training dataset to assign to the custom model.                             |
| `holdout_dataset_id`           | str  | The ID of the holdout dataset to assign to the custom model.                              |
| `custom_model_folder`          | str  | The ID of the holdout dataset to assign to the custom model.                              |
| `create_from_previous`         | bool | If set to True, this parameter creates a custom model version containing files from a previous version. |


Sample DAG config params:

```
"is_major_update" - The flag defining if a custom model version will be a minor or a major version.
"files" - The list of tuples, where values in each tuple are the local filesystem path and
            the path the file should be placed in the model.
"files_to_delete" - The list of a file items IDs to be deleted.
"network_egress_policy": - Determines whether the given custom model is isolated, or can access the public network.
"maximum_memory": - The maximum memory that might be allocated by the custom-model.
"replicas": - A fixed number of replicas that will be deployed in the cluster.
"required_metadata_values" - Additional parameters required by the execution environment.
"keep_training_holdout_data" - If the version should inherit training and holdout data from the previous version.
```

For more [custom inference model creation parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/mlops/custom_model.html?highlight=CustomInferenceModel#create-custom-inference-model), see the DataRobot documentation.

---

#### `CustomModelTestOperator`

Create and start a custom model test.

Returns a created custom model test ID

Parameters:

| Parameter                 | Type | Description                                                                                                         |
|---------------------------|------|---------------------------------------------------------------------------------------------------------------------|
| `custom_model_id`         | str  | The ID of the custom model.                                                                                         |
| `custom_model_version_id` | str  | The ID of the custom model version.                                                                                 |
| `dataset_id`              | str  | The id of the testing dataset for non-unstructured custom models. Ignored and not required for unstructured models. |

Sample DAG config params:

```
"network_egress_policy": - Determines whether the given custom model is isolated, or can access the public network.
"maximum_memory": - The maximum memory that might be allocated by the custom-model.
"replicas": - A fixed number of replicas that will be deployed in the cluster.
```

For more [custom model test creation parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/mlops/custom_model.html?highlight=CustomModelTest#create-custom-model-test), see the DataRobot documentation.

---

#### `GetCustomModelTestOverallStatusOperator`

Get a custom model testing overall status.

Returns a custom model test overall status

Parameters:

| Parameter                     | Type | Description                            |
|-------------------------------|------|----------------------------------------|
| `custom_model_test_id`        | str  | The ID of the custom model test.       |

For more [custom model test get status parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/mlops/custom_model.html?highlight=CustomModelTest#retrieve-custom-model-test), see the DataRobot documentation.

---

#### `CreateCustomModelDeploymentOperator`

Create a deployment from a DataRobot custom model image.

Returns the created deployment id

Parameters:

| Parameter                      | Type | Description                                                                      |
|--------------------------------|------|----------------------------------------------------------------------------------|
| `custom_model_version_id`      | str  | The id of the DataRobot custom model version to deploy                           |
| `deployment_name`              | str  | a human readable label (name) of the deployment                                  |
| `default_prediction_server_id` | str  | an identifier of a prediction server to be used as the default prediction server |
| `description`                  | str  | a human readable description of the deployment                                   |
| `importance`                   | str  | deployment importance                                                            |

For more [create_from_custom_model_version parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=create_from_custom_model_version#datarobot.models.Deployment.create_from_custom_model_version), see the DataRobot documentation.

---

#### `GetDeploymentModelOperator`

Gets current model info from a deployment.

Returns a model info from a Deployment

Parameters:

| Parameter         | Type | Description                   |
|-------------------|------|-------------------------------|
| `deployment_id`   | str  | DataRobot deployment ID       |

For more [get deployment parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=Deployment.get#datarobot.models.Deployment.get), see the DataRobot documentation.

---

#### `ReplaceModelOperator`

Replaces the current model for a deployment.

Returns a model info from a Deployment

Parameters:

| Parameter       | Type | Description                                                                                                                                                                                                                         |
|-----------------|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `deployment_id` | str  | DataRobot deployment ID                                                                                                                                                                                                             |
| `new_model_id`  | str  | The id of the new model to use. If replacing the deployment's model with a CustomInferenceModel, a specific CustomModelVersion ID must be used.                                                                                     |
| `reason`        | str  | The reason for the model replacement. Must be one of 'ACCURACY', 'DATA_DRIFT', 'ERRORS', 'SCHEDULED_REFRESH', 'SCORING_SPEED', or 'OTHER'. This value will be stored in the model history to keep track of why a model was replaced |

For more [replace_model parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=replace_model#datarobot.models.Deployment.replace_model), see the DataRobot documentation.

---

#### `ActivateDeploymentOperator`

Activate or deactivate a Deployment.

Returns the Deployment status (active/inactive)

Parameters:

| Parameter               | Type | Description                                                                   |
|-------------------------|------|-------------------------------------------------------------------------------|
| `deployment_id`         | str  | DataRobot deployment ID                                                       |
| `activate`              | str  | If set to True, this parameter activates the deployment. Set to False to deactivate the deployment. |

For more [activate deployment parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=replace_model#datarobot.models.Deployment.activate), see the DataRobot documentation.

---

#### `GetDeploymentStatusOperator`

Get the deployment status (active or inactive).

Returns the deployment status.

Parameters:

| Parameter       | Type | Description              |
|-----------------|------|--------------------------|
| `deployment_id` | str  | An identifier for the deployed model.  |

For more [deployment parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=replace_model#datarobot.models.Deployment.activate), see the DataRobot documentation.

---

#### `RelationshipsConfigurationOperator`

Creates a relationship configuration.

Returns the relationships configuration ID.

Parameters:

| Parameter                     | Type | Description                                                                                                                                                |
|-------------------------------|------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dataset_definitions`         | str  | A list of dataset definitions. Each element is a dict retrieved from the `DatasetDefinitionOperator` operator.                                                      |
| `relationships`               | str  | A list of relationships. Each element is a dict retrieved from DatasetRelationshipOperator operator.                                                          |
| `feature_discovery_settings`  | str  | Optional. A list of Feature Discovery settings. If not provided, it will be retrieved from the DAG configuration parameters. Otherwise, default settings are used. |


For more [Feature Discovery parameters](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/data/feature_discovery.html?highlight=Featue%20Discovery#feature-discovery), see the DataRobot documentation.

---

#### `DatasetDefinitionOperator`

Creates a dataset definition for Feature Discovery.

Returns a dataset definition dict.

Parameters:

| Parameter              | Type | Description                                                                                                      |
|------------------------|------|------------------------------------------------------------------------------------------------------------------|
| `dataset_identifier`   | str  | The alias of the dataset, used directly as part of the generated feature names.                                      |
| `dataset_id`           | str  | The identifier of the dataset in the AI Catalog.                                                                |
| `dataset_version_id`   | str  | The identifier of the dataset version in the AI Catalog.                                                        |
| `primary_temporal_key` | str  | The name of the column indicating the time of record creation.                                                            |
| `feature_list_id`      | str  | Specifies the feature list to use.                                                                             |
| `snapshot_policy`      | str  | The policy to use when creating a project or making predictions. If omitted, the endpoint will use 'latest' by default. |


For more [create-dataset-definitions-and-relationships-using-helper-functions](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/data/feature_discovery.html?highlight=DatasetDefinition#create-dataset-definitions-and-relationships-using-helper-functions), see the DataRobot documentation.

---

#### `DatasetRelationshipOperator`

Create a relationship between datasets defined in DatasetDefinition.

Returns a dataset definition dict.

Parameters:

| Parameter                             | Type       | Description                                                                                                                                                                                                                                                                             |
|---------------------------------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dataset1_identifier`                 | List[str]  | Identifier of the first dataset in this relationship. This is specified in the identifier field of the `dataset_definition` structure. If set to None, then the relationship is with the primary dataset.                                                                                            |
| `dataset2_identifier`                 | List[str]  | Identifier of the second dataset in this relationship. This is specified in the identifier field of the `dataset_definition` schema.                                                                                                                                                          |
| `dataset1_keys`                       | List[str]  | A list of strings (max length: 10 min length: 1). The column(s) from the first dataset which are used to join to the second dataset.                                                                                                                                                            |
| `dataset2_keys`                       | List[str]  | A list of strings (max length: 10 min length: 1). The column(s) from the second dataset that are used to join to the first dataset.                                                                                                                                                             |
| `feature_derivation_window_start`     | int        | How many time units of each dataset's primary temporal key into the past relative to the datetimePartitionColumn the feature derivation window should begin. Will be a negative integer, If present, the feature engineering graph performs time-aware joins.                       |
| `feature_derivation_window_end`       | int        | Determines how many units of time of each dataset's record primary temporal key into the past relative to the datetimePartitionColumn the feature derivation window should end.  It is a non-positive integer if present. If present, the feature engineering graph performs time-aware joins. |
| `feature_derivation_window_time_unit` | str        | The unit of time the feature derivation window. One of ``datarobot.enums.AllowedTimeUnitsSAFER`` If present, time-aware joins will be used. Only applicable when dataset1_identifier is not provided.                                                                                       |
| `feature_derivation_windows`          | List       | A list of feature derivation windows settings. If present, time-aware joins will be used. Only allowed when `feature_derivation_window_start`, `feature_derivation_window_end `, and `feature_derivation_window_time_unit` are not provided.                                                      |
| `prediction_point_rounding`           | List[dict] | Closest value of `prediction_point_rounding_time_unit` to round the prediction point into the past when applying the feature derivation if present. Only applicable when `dataset1_identifier` is not provided.                                                                                    |
| `prediction_point_rounding_time_unit` | str        | Time unit of the prediction point rounding. One of ``datarobot.enums.AllowedTimeUnitsSAFER``. Only applicable when `dataset1_identifier` is not provided.                                                                                                                                  |


For more [create-dataset-definitions-and-relationships-using-helper-functions](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/data/feature_discovery.html?highlight=DatasetDefinition#create-dataset-definitions-and-relationships-using-helper-functions), see the DataRobot documentation.

---

#### `ComputeFeatureImpactOperator`

Creates a Feature Impact job in DataRobot.

Returns a Feature Impact job ID.

Parameters:

| Parameter       | Type | Description           |
|-----------------|------|-----------------------|
| `project_id`    | str  | DataRobot project ID.  |
| `model_id`      | str  | DataRobot model ID.    |

For more [request_feature_impact](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=request_feature_impact#datarobot.models.Model.request_feature_impact), see the DataRobot documentation.

---

#### `ComputeFeatureEffectsOperator`

Submit a request to compute Feature Effects for the model.

Returns the Feature Effects job ID.

Parameters:

| Parameter        | Type | Description          |
|------------------|------|----------------------|
| `project_id`     | str  | DataRobot project ID. |
| `model_id`       | str  | DataRobot model ID.   |

For more [request_feature_impact](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=request_feature_effect#datarobot.models.DatetimeModel.request_feature_effect), see the DataRobot documentation.

---

#### `ComputeShapOperator`

Submit request to compute SHAP impact job for the model.

Returns SHAP impact job ID

Parameters:

| Parameter      | Type | Description             |
|----------------|------|-------------------------|
| `project_id`   | str  | DataRobot project ID    |
| `model_id`     | str  | DataRobot model ID      |

For more [shap-impact](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/modeling/model.html?highlight=ShapImpact#shap-impact), see the DataRobot documentation.

---

#### `CreateExternalModelPackageOperator`

Create an external model package in DataRobot MLOps from JSON configuration.

Returns a model package ID of newly created ModelPackage.

Parameters:

| Parameter      | Type | Description                                 |
|----------------|------|---------------------------------------------|
| `model_info`   | str  | A JSON object of external model parameters. |

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

---

#### `DeployModelPackageOperator`

Create a deployment from a DataRobot model package.

Returns The created deployment ID

Parameters:

| Parameter                      | Type           | Description                                                                                                                                                                      |
|--------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `deployment_name`              | str            | A human readable label of the deployment.                                                                                                                                        |
| `model_package_id`             | str            | The ID of the DataRobot model package to deploy.                                                                                                                                 |
| `default_prediction_server_id` | str            | An identifier of a prediction server to be used as the default prediction server. When working with prediction environments, default prediction server Id should not be provided |
| `prediction_environment_id`    | str            | An identifier of a prediction environment to be used for model deployment.                                                                                                       |
| `description`                  | str            | A human readable description of the deployment.                                                                                                                                  |
| `importance`                   | str            | Deployment importance level.                                                                                                                                                     |
| `user_provided_id`             | str            | A user-provided unique ID associated with a deployment definition in a remote git repository.                                                                                    |
| `additional_metadata`          | Dict[str, str] | A Key/Value pair dict, with additional metadata.                                                                                                                                 |

---

#### `AddExternalDatasetOperator`

Upload a new dataset from a catalog dataset to make predictions for a model

Returns external dataset ID for the model

Parameters:

| Parameter            | Type | Description                              |
|----------------------|------|------------------------------------------|
| `project_id`         | str  | DataRobot project ID                     |
| `dataset_id`         | str  | DataRobot AI Catalog dataset ID          |
| `credential_id`      | str  | DataRobot Credentials ID                 |
| `dataset_version_id` | str  | DataRobot AI Catalog dataset version ID  |

For more [upload_dataset_from_catalog](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=upload_dataset_from_catalog#datarobot.models.Project.upload_dataset_from_catalog), see the DataRobot documentation.

---

#### `RequestModelPredictionsOperator`

Requests predictions against a previously uploaded dataset.

Returns model predictions job ID

Parameters:

| Parameter             | Type | Description                   |
|-----------------------|------|-------------------------------|
| `project_id`          | str  | DataRobot project ID          |
| `model_id`            | str  | DataRobot model ID            |
| `external_dataset_id` | str  | DataRobot external dataset ID |

For more [request_predictions](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=request_predictions#datarobot.models.Model.request_predictions), see the DataRobot documentation.

---

#### `TrainModelOperator`

Submit a job to the queue to train a model from specific blueprint.

Returns model training job ID

Parameters:

| Parameter              | Type | Description                                                                                                                                             |
|------------------------|------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `project_id`           | str  | DataRobot project ID                                                                                                                                    |
| `blueprint_id`         | str  | DataRobot blueprint ID                                                                                                                                  |
| `featurelist_id`       | str  | The identifier of the featurelist to use. If not defined, the default for this project is used.                                                         |
| `source_project_id`    | str  | Which project created this blueprint_id. If ``None``, it defaults to looking in this project. Note that you must have read permissions in this project. |

Example of DAG config params:
{
    "sample_pct":
    "scoring_type":
    "training_row_count":
    "n_clusters":
}

For more [start-training-a-model](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/modeling/model.html?highlight=project.train#start-training-a-model), see the DataRobot documentation.

---

#### `RetrainModelOperator`

Submit a job to the queue to retrain a model on a specific sample size and/or custom featurelist.

Returns a model retraining job ID

Parameters:

| Parameter            | Type | Description                                                                                     |
|----------------------|------|-------------------------------------------------------------------------------------------------|
| `project_id`         | str  | DataRobot project ID                                                                            |
| `model_id`           | str  | DataRobot model ID                                                                              |
| `featurelist_id`     | str  | The identifier of the featurelist to use. If not defined, the default for this project is used. |

Example of DAG config params:
{
    "sample_pct":
    "scoring_type":
    "training_row_count":
}

For more [train-a-model-on-a-different-sample-size](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/modeling/model.html?highlight=model.train#train-a-model-on-a-different-sample-size), see the DataRobot documentation.

---

#### `PredictionExplanationsInitializationOperator`

Triggering a prediction explanations initialization of a model.

Returns a Prediction Explanations Initialization job ID

Parameters:

| Parameter            | Type | Description          |
|----------------------|------|----------------------|
| `project_id`         | str  | DataRobot project ID |
| `model_id`           | str  | DataRobot model ID   |

For more [prediction-explanations](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=PredictionExplanationsInitialization#prediction-explanations), see the DataRobot documentation.

---

#### `ComputePredictionExplanationsOperator`

Create prediction explanations for the specified dataset.

Returns a Triggered prediction explanations for the specified dataset job ID

Parameters:

| Parameter             | Type | Description                    |
|-----------------------|------|--------------------------------|
| `project_id`          | str  | DataRobot project ID           |
| `model_id`            | str  | DataRobot model ID             |
| `external_dataset_id` | str  | DataRobot external dataset ID  |

Example of DAG config params:

{
    "max_explanations"
    "threshold_low"
    "threshold_high"
}

For more [prediction-explanations](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=PredictionExplanationsInitialization#prediction-explanations), see the DataRobot documentation.

---

### [Sensors](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/sensors/datarobot.py)

#### `AutopilotCompleteSensor`

Checks if Autopilot is complete.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `project_id`                | str  | The DataRobot project ID. |

---

#### `ScoringCompleteSensor`

Checks if batch scoring is complete.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `job_id`                    | str  | The batch prediction job ID. |

---

#### `MonitoringJobCompleteSensor`

Checks if a monitoring job is complete.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `job_id`                    | str  | The batch monitoring job ID. |

---

#### `BaseAsyncResolutionSensor`

Checks if the DataRobot Async API call is complete.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `job_id`                    | str  | The DataRobot async API call status check ID. |

---

#### `DataRobotJobSensor`

Checks whether DataRobot Job is complete.

Parameters:

| Parameter                   | Type | Description           |
|-----------------------------|------|-----------------------|
| `project_id`                | str  | DataRobot project ID. |
| `job_id`                    | str  | DataRobot Job ID.     |

---

#### `ModelTrainingJobSensor`

Checks whether DataRobot Model Training Job is complete.

Returns False if job not yet completed, PokeReturnValue(True, trained_model.id) if model training completed

Parameters:

| Parameter                   | Type | Description           |
|-----------------------------|------|-----------------------|
| `project_id`                | str  | DataRobot project ID. |
| `job_id`                    | str  | DataRobot Job ID.     |

----

### [Hooks](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/hooks/datarobot.py)

#### `DataRobotHook`

A hook to initialize the DataRobot Public API client.

----

## Pipeline

The modules described above allow you to construct a standard DataRobot pipeline in an Airflow DAG:

    create_project_op >> train_models_op >> autopilot_complete_sensor >> deploy_model_op >> score_predictions_op >> scoring_complete_sensor

## Example DAGS

See the the [`datarobot_provider/example_dags`](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/example_dags) directory for the example DAGs.

You can find the following examples using a preconfigured connection in the `datarobot_provider/example_dags` directory:

| Example DAG                                            | Description                                                                             |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `datarobot_pipeline_dag.py`                            | Run the basic end-to-end workflow in DataRobot.                                         |
| `datarobot_score_dag.py`                               | Perform DataRobot batch scoring.                                                        |
| `datarobot_jdbc_batch_scoring_dag.py`                  | Perform DataRobot batch scoring with a JDBC data source.                                |
| `datarobot_aws_s3_batch_scoring_dag.py`                | Use DataRobot AWS Credentials with `ScorePredictionsOperator`.                          |
| `datarobot_gcp_storage_batch_scoring_dag.py`           | Use DataRobot GCP Credentials with `ScorePredictionsOperator`.                          |
| `datarobot_bigquery_batch_scoring_dag.py`              | Use DataRobot GCP Credentials with `ScorePredictionsOperator`.                          |
| `datarobot_azure_storage_batch_scoring_dag.py`         | Use DataRobot Azure Storage Credentials with `ScorePredictionsOperator`.                |
| `datarobot_jdbc_dataset_dag.py`                        | Upload a dataset to the AI Catalog through a JDBC connection.                           |
| `datarobot_batch_monitoring_job_dag.py`                | Run a batch monitoring job.                                                             |
| `datarobot_create_project_from_ai_catalog_dag.py`      | Create a DataRobot project from a DataRobot AI Catalog dataset.                         |
| `datarobot_create_project_from_dataset_version_dag.py` | Create a DataRobot project from a specific dataset version in the DataRobot AI Catalog. |
| `datarobot_dataset_new_version_dag.py`                 | Create a new version of an existing dataset in the DataRobot AI Catalog.                |
| `datarobot_dataset_upload_dag.py`                      | Upload a local file to the DataRobot AI Catalog.                                        |
| `datarobot_get_datastore_dag.py`                       | Create a DataRobot data store with `GetOrCreateDataStoreOperator`.                       |
| `datarobot_jdbc_dataset_dag.py`                        | Create a DataRobot project from a JDBC data source.                                     |
| `datarobot_jdbc_dynamic_dataset_dag.py`                | Create a DataRobot project from a JDBC dynamic data source.                             |
| `datarobot_upload_actuals_catalog_dag.py`              | Upload actuals from the DataRobot AI Catalog.                                           |
| `deployment_service_stats_dag.py`                      | Get a deployment's service statistics with `GetServiceStatsOperator`.                    |
| `deployment_stat_and_accuracy_dag.py`                  | Get a deployment's service statistics and accuracy.                                     |
| `deployment_update_monitoring_settings_dag.py`         | Update a deployment's monitoring settings.                                              |
| `deployment_update_segment_analysis_settings_dag.py`   | Update a deployment's segment analysis settings.                                        |
| `download_scoring_code_from_deployment_dag.py`         | Download a Scoring Code JAR file from a DataRobot deployment.                           |
| `advanced_datarobot_pipeline_jdbc_dag.py`              | Run the advanced end-to-end workflow in DataRobot.                                      |
| `datarobot_autopilot_options_pipeline_dag.py`          | Creates a DataRobot project and starts Autopilot with advanced options.                   |
| `datarobot_custom_model_pipeline_dag.py`               | Create an end-to-end workflow with custom models in DataRobot.                           |
| `datarobot_custom_partitioning_pipeline_dag.py`        | Create a custom partitioned project and train models.                              |
| `datarobot_datetime_partitioning_pipeline_dag.py`      | Create a datetime partitioned project.                            |
| `datarobot_external_model_pipeline_dag.py`             | An end-to-end workflow with external models in DataRobot.                         |
| `datarobot_feature_discovery_pipeline_dag.py`          | Create a Feature Discovery project and train models.                     |
| `datarobot_timeseries_pipeline_dag.py`                 | Create a time series DataRobot project.                            |
| `deployment_activate_deactivate_dag.py`                | An example of deployment activation/deactivation and getting deployment status.                 |
| `deployment_replace_model_dag.py`                      | An example of model replacement for deployments.                                                |
| `model_compute_insights_dag.py`                        | An example of computing Feature Impact and Feature Effects.                                |
| `model_compute_prediction_explanations_dag.py`         | An example of a compute prediction explanations job.                                         |
| `model_compute_predictions_dag.py`                     | An example of computing predictions for model.                                               |
| `model_compute_shap_dag.py`                            | An example of computing SHAP.                                                            |
| `model_retrain_dag.py`                                 | Example of model retraining job on a specific sample size/featurelist.                    |
| `model_train_dag.py`                                   | Example of model training job based on specific blueprint.                              |

The advanced end-to-end workflow in DataRobot (`advanced_datarobot_pipeline_jdbc_dag.py`) contains the following steps:

- Ingest a dataset to the AI Catalog from JDBC datasource
- Create a DataRobot project
- Train models using Autopilot
- Deploy the recommended model
- Change deployment settings (enable monitoring settings, segment analysis, and bias and fairness)
- Run batch scoring using a JDBC datasource
- Upload actuals from a JDBC datasource
- Collect deployment metrics: service statistics, features drift, target drift, accuracy and process it with custom python operator.

## Issues

Please submit [issues](https://github.com/datarobot/airflow-provider-datarobot/issues) and [pull requests](https://github.com/datarobot/airflow-provider-datarobot/pulls) in our official repo:
[https://github.com/datarobot/airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot)

We are happy to hear from you. Please email any feedback to the authors at [support@datarobot.com](mailto:support@datarobot.com).


# Copyright Notice

Copyright 2023 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
