# Example Airflow Directed Acyclic Graphs (DAGs)

This section provides several examples of DAGs that are designed for specialized use cases.

* **Dataprep and Autopilot**: creates a project, trains a model, and registers it to DataRobot MLOps.
* **Deployment Prediction Generation**: creates a new deployment from a registered model and demonstrates how to generate predictions from the deployment.
* **Model Training XGBoost**: wrangles Snowflake data, creates a new project, trains an XGBoost model from a selected blueprint, and registers it to DataRobot MLOps for use as a future deployment.
* **Custom Function**: demonstrates how to make and format predictions from a model trained in DataRobot and push the formatted predictions to an external data storage.

Refer to each individual page for more details on each example DAG.

## Load example DAGs into Airflow {: #load-example-dags-into-airflow }

The example DAGs do not appear on the **DAGs** page by default.
To make the DataRobot provider for Apache Airflow's example DAGs available:

1. Download the DAG files from the [airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot/tree/main/datarobot_provider/example_dags){ target=_blank } repository.

2. Copy the [`datarobot_provider/example_dags` Airflow Example DAGs directory](https://github.com/datarobot/airflow-provider-datarobot/blob/main/datarobot_provider/example_dags){ target=_blank } to your project.

3. Wait a minute or two and refresh the page. The example DAGs appear on the **DAGs** page, including the **datarobot_pipeline** DAG.

(example-dag-reference)=

```{toctree}
:glob: true
:maxdepth: 1

*
```