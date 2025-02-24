# Hospital Readmissions Example

> **datarobot_provider/examples/hospital_readmissions_example.py**

```{admonition} Required Feature Flags
* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS
```

The `hospital_readmissions_example` DAG demonstrates how to use the DataRobot provider for Apache Airflow to
create a project, train a model, and register it to DataRobot MLOps. The initial demo flow utilizes a public csv
file with hospital readmissions data.

This example covers the following steps:
* Create or reuse a use case
* Upload a dataset
* Create a wrangler recipe
* Publish and run the wrangler recipe
* Create a new project/experiment in the use case
* Run autopilot in quick mode
* Wait for autopilot to complete before continuing
* Select the top model
* Register the model for deployment
