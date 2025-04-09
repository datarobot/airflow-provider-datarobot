# Deployment Prediction Generation
`path: datarobot_provider/examples/deployment_prediction_generation.py`

This is an example of an Aiflow DAG for DataRobot data deployment and prediction generation.
This DAG will create a new deployment from a registered model and demonstrate how to generate predictions from the deployment.

## Requirements

* ENABLE_MLOPS

## Input parameters
| Parameter                     | Data Types | Required | Description                                                                       |
|-------------------------------|------------|----------|-----------------------------------------------------------------------------------|
| model_package_id              | str        | Yes      | The ID of the DataRobot model package (version) to deploy.                        |
| deployment_label              | str        | Yes      | A human readable label for the deployment.                                        |
| default_prediction_server_id  | str        | Yes      | An identifier of a prediction server to be used as the default prediction server. |
| target_drift_enabled          | bool       | No       | If target drift tracking is to be turned on.                                      |
| feature_drift_enabled         | bool       | No       | If feature drift tracking is to be turned on.                                     |
| predictions_dataset_file_path | str        | Yes      | The path to the dataset to be used for predictions.                               |

## Usage

1. Create or reuse a use case
2. Deploy the registered model
3. Update drift tracking settings
4. Upload the dataset to be used for predictions
5. Apply a wrangling recipe to the dataset
6. Make predictions using the deployed model
7. Wait for the predictions to finish scoring

## Result

You now have a new deployment created from a registered model and test making predictions from the new deployment.

## Troubleshooting
In order to make predictions from a deployment via DataRobot's Prediction API, you need a prediction server ID.
See [Get a prediction server ID](https://docs.datarobot.com/en/docs/api/reference/predapi/pred-server-id.html){ target=_blank } for more information on how to get the prediction server ID for your organization or deployments.
