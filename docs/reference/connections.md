# Connections
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