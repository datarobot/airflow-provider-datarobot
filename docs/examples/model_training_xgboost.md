---
title: Model Training XGBoost
description: Train an XGBoost model from a new project and register the model for deployment.
path: datarobot_provider/examples/model_training_xgboost.py

---

# Model Training XGBoost
`datarobot_provider/examples/model_training_xgboost.py`

This section summarizes the purpose and/or use of the DAG in a couple sentences.
Unless otherwise necessary, try not to exceed four sentences, and keep each sentence on its own line in the code (it will still render as a paragraph).

## Requirements

* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS

## Input parameters

| Parameter | Data Types | Required | Description |
|-----------|------------| -------- | ----------- |
| Param1 | Data type | Yes | Describe `param1`. |
| Param2 | Data type | No | Describe `param2`. |

## Procedure (Title TBD)

1. Create or reuse a use case
2. Upload a dataset
3. Create a wrangler recipe
4. Publish and run the wrangler recipe
5. Create a new project/experiment in the use case
6. Run autopilot in manual mode
7. Get a list of available blueprints
8. Select an xgboost blueprint
9. Train the xgboost model
10. Register the model for deployment
11. Compute SHAP insights on the xgboost model

## Result (TBD)

A completed project with a trained XGBoost model that is registered and ready for deployment.
