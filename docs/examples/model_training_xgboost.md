# Single Model Training Example

> **datarobot_provider/examples/model_training_xgboost.py**

```{admonition} Required Feature Flags
* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS
```

The `model_training_xgboost` DAG demonstrates how to use the DataRobot provider for Apache Airflow to create a
project, train a model from a selected blueprint, and register it to DataRobot MLOps.

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
