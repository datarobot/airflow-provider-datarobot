# Single Model Training Example

> **datarobot_provider/examples/hospital_readmissions_xgboost_example.py**

```{admonition} Required Feature Flags
* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS
```

The `hospital_readmissions_xgboost_example` DAG demonstrates how to use the DataRobot provider for Apache Airflow to create a project, train a model, and register it to DataRobot MLOps.
This flow is similar to`datarobot_provider/examples/hospital_readmissions_example.py`, except that it trains a single model based on the model type and uses the manual mode.
The `hospital_readmissions_example.py` flow uses a different set of operators for training.

This example covers the following steps:
* Create or reuse a use case
* Upload a dataset
* Create a wrangler recipe
* Publish and run the wrangler recipe
* Create a new project/experiment in the use case
* Run autopilot in manual mode
* Get a list of available blueprints
* Select an xgboost blueprint
* Train the xgboost model
* Register the model for deployment
* Compute SHAP insights on the xgboost model
