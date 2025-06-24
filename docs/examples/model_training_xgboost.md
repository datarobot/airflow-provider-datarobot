# Model Training XGBoost

`path: datarobot_provider/example_dags/model_training_xgboost.py`

This DAG demonstrates how to use the DataRobot provider for Apache Airflow to wrangle Snowflake data, create a new project, train an XGBoost model from a selected blueprint, and register it to DataRobot for use as a future deployment.

## Requirements

* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS

## Input parameters

| Parameter | Data Types | Required | Description                                                        |
|-----------|------------|----------|--------------------------------------------------------------------|
| data_connection | str        | Yes      | Name of the snowflake connection.                                  |
| table_schema | str        | Yes      | Snowflake table schema.                                            |
| primary_table | str        | Yes      | Snowflake primary table.                                           |
| secondary_table | str        | Yes      | Snowflake secondary table to join to the primary table.            |
| project_name | str        | No       | Name of the project created.                                       |
| autopilot_settings | dict       | Yes      | Dictionary of autopilot parameters. Only `target` must be defined. |

## Usage

1. Create or reuse a use case
2. Connect to a Snowflake data source
3. Create a wrangler recipe
4. Publish and run the wrangler recipe
5. Create a new project/experiment in the use case
6. Run autopilot in manual mode
7. Get a list of available blueprints
8. Select an XGBoost blueprint
9. Train the XGBoost model
10. Register the model for deployment
11. Compute SHAP insights on the XGBoost model

## Result

A completed project with a trained XGBoost model that is registered and ready for deployment.
