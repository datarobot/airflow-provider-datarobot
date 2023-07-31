# DataRobot Provider for Apache Airflow

This package provides operators, sensors, and a hook to integrate [DataRobot](https://www.datarobot.com) into Apache Airflow.
Using these components, you should be able to build the essential DataRobot pipeline - create a project, train models, deploy a model,
and score predictions against the model deployment.

## Install the Airflow Provider

The DataRobot provider for Apache Airflow requires an environment with the following dependencies installed:

* [Apache Airflow](https://pypi.org/project/apache-airflow/) >= 2.3

* [DataRobot Python API Client](https://pypi.org/project/datarobot/) >= 3.2.0b1

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
            "credential_id": "62160b511fb29da8dd5f2c81"
        },
        "output_settings": {
            "type": "s3",
            "url": "s3://path/to/results-dir/Diabetes10k_predictions.csv",
            "credential_id": "62160b511fb29da8dd5f2c81"
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
and you should use this output value as a `dataset_id` argument in `CreateProjectOperator` object creation step.

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

- Use `GetOrCreateCredentialOperator` to pass `credential_id` from preconfigured DataRobot Credentials (Airflow Connections)
    or you can manually set `credential_id` parameter in the config. [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
- OR a Dataset ID in the AI Catalog.
- OR a DataStore ID for JDBC source connection, you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from preconfigured Airflow Connection.

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
        "credential_id": "62160b511fb29da8dd5f2c81"
    },
    "output_settings": {
        "type": "s3",
        "url": "s3://my-bucket/Diabetes10k_predictions.csv",
        "credential_id": "62160b511fb29da8dd5f2c81"
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
    "output_settings": {
        ...
    }
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
| `deployment_id`             | str  | THe DataRobot deployment ID. |

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

Returns a dict with the accuracy for a Deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

No config params are required; however, the [optional params](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_accuracy) may be passed in the config as follows:

``` json
"accuracy": { }
```

---

#### `GetBiasAndFairnessSettingsOperator`

Gets the Bias And Fairness settings for deployment.

Returns a dict with the [Bias And Fairness settings for a Deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_bias_and_fairness_settings)

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

No config params are required.

---

#### `UpdateBiasAndFairnessSettingsOperator`

Updates the Bias And Fairness settings for deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

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

Returns a dict with the [segment analysis settings for a Deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_segment_analysis_settings)

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

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
| `deployment_id`             | str  | THe DataRobot deployment ID. |

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
| `association_id_settings` | The [association ID settings for this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.Deployment.get_association_id_settings) |
| `predictions_data_collection_settings` | The [predictions data collection settings of this deployment](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html?highlight=predictions_data_collection_settings#datarobot.models.Deployment.get_predictions_data_collection_settings) |

---
    
#### `UpdateMonitoringSettingsOperator`

Updates monitoring settings for a deployment.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | THe DataRobot deployment ID. |

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

- You can use `GetOrCreateCredentialOperator` to pass `credential_id` from preconfigured DataRobot Credentials (Airflow Connections)
    or you can manually set `credential_id` parameter in the config. [S3 credentials added to DataRobot via Python API client](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/reference/admin/credentials.html#s3-credentials).
- OR a Dataset ID in the AI Catalog
- OR a DataStore ID for JDBC source connection, you can use `GetOrCreateDataStoreOperator` to pass `datastore_id` from preconfigured Airflow Connection

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |
| `datastore_id`              | str  | The DataRobot datastore ID. |
| `credential_id`             | str  | The DataRobot credentials ID. |

Sample config params:

<details>

<summary>Sample config</summary

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
```

</details>

For more [batch monitoring settings](https://datarobot-public-api-client.readthedocs-hosted.com/en/latest-release/autodoc/api_reference.html#datarobot.models.BatchMonitoringJob.run), see the DataRobot documentation.

---

#### `DownloadModelScoringCodeOperator`

Downloads scoring code artifact from a Model.

Parameters:

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `project_id`                | str  | The DataRobot project ID. |
| `model_id`                  | str  | The DataRobot model ID. |
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

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |
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

| Parameter                   | Type | Description |
|-----------------------------|------|-------------|
| `deployment_id`             | str  | The DataRobot deployment ID. |
| `dataset_id`                | str  | The DataRobot AI Catalog dataset ID. |
| `dataset_version_id`        | str  | The DataRobot AI Catalog dataset version ID. |

Sample config params:

```
"association_id_column": "id",
"actual_value_column": "ACTUAL",
"timestamp_column": "timestamp",
```

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

| Example DAG                                            | Description |
|--------------------------------------------------------|-------------|
| `datarobot_pipeline_dag.py`                            | Run the basic end-to-end workflow in DataRobot. |
| `datarobot_score_dag.py`                               | Perform DataRobot batch scoring. |
| `datarobot_jdbc_batch_scoring_dag.py`                  | Perform DataRobot batch scoring with a JDBC data source. |
| `datarobot_aws_s3_batch_scoring_dag.py`                | Use DataRobot AWS Credentials with `ScorePredictionsOperator`. |
| `datarobot_gcp_storage_batch_scoring_dag.py`           | Use DataRobot GCP Credentials with `ScorePredictionsOperator`. |
| `datarobot_bigquery_batch_scoring_dag.py`              | Use DataRobot GCP Credentials with `ScorePredictionsOperator`. |
| `datarobot_azure_storage_batch_scoring_dag.py`         | Use DataRobot Azure Storage Credentials with `ScorePredictionsOperator`. |
| `datarobot_jdbc_dataset_dag.py`                        | Upload a dataset to the AI Catalog through a JDBC connection.  |
| `datarobot_batch_monitoring_job_dag.py`                | Run a batch monitoring job. |
| `datarobot_create_project_from_ai_catalog_dag.py`      | Create a DataRobot project from a DataRobot AI Catalog dataset. |
| `datarobot_create_project_from_dataset_version_dag.py` | Create a DataRobot project from a specific dataset version in the DataRobot AI Catalog. |
| `datarobot_dataset_new_version_dag.py`                 | Create a new version of an existing dataset in the DataRobot AI Catalog. |
| `datarobot_dataset_upload_dag.py`                      | Upload a local file to the DataRobot AI Catalog. |
| `datarobot_get_datastore_dag.py`                       | Create a DataRobot DataStore with `GetOrCreateDataStoreOperator`. |
| `datarobot_jdbc_dataset_dag.py`                        | Create a DataRobot project from a JDBC data source. |
| `datarobot_jdbc_dynamic_dataset_dag.py`                | Create a DataRobot project from a JDBC dynamic data source. |
| `datarobot_upload_actuals_catalog_dag.py`              | Upload actuals from the DataRobot AI Catalog. |
| `deployment_service_stats_dag.py`                      | Get a deployment's service statistics with `GetServiceStatsOperator` |
| `deployment_stat_and_accuracy_dag.py`                  | Get a deployment's service statistics and accuracy. |
| `deployment_update_monitoring_settings_dag.py`         | Update a deployment's monitoring settings. |
| `deployment_update_segment_analysis_settings_dag.py`   | Update a deployment's segment analysis settings. |
| `download_scoring_code_from_deployment_dag.py`         | Download scoring code (JAR file) from a DataRobot deployment. |
| `advanced_datarobot_pipeline_jdbc_dag.py`              | Run the advanced end-to-end workflow in DataRobot. |

The advanced end-to-end workflow in DataRobot (`advanced_datarobot_pipeline_jdbc_dag.py`) contains the following steps:

- Ingest a dataset to the AI Catalog from JDBC datasource
- Create a DataRobot Project
- Train models using Autopilot
- Deploy the Recommended model
- Change Deployment settings (enable monitoring settings, segment analysis, and bias and fairness)
- Run Batch Scoring using JDBC datasource
- Upload Actuals from JDBC datasource
- Collect Deployment metrics: service statistics, features drift, target drift, accuracy and process it with custom python operator.

## Issues

Please submit [issues](https://github.com/datarobot/airflow-provider-datarobot/issues) and [pull requests](https://github.com/datarobot/airflow-provider-datarobot/pulls) in our official repo:
[https://github.com/datarobot/airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot)

We are happy to hear from you. Please email any feedback to the authors at [support@datarobot.com](mailto:support@datarobot.com).


# Copyright Notice

Copyright 2023 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
