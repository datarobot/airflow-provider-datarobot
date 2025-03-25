# Custom Function Example DAG
`path: datarobot_provider/examples/post_predictions_formatting.py`

This DAG demonstrates how to construct a custom function example DAG that can utilize the DataRobot APIs to
perform any operation. Specifically, the example demonstrates how to make and format predictions from a model
trained in DataRobot and push the formatted predictions to an external data storage.

## Requirements

_N/A_

## Input parameters
| Parameter | Data Types | Required | Description                                                  |
|-----------|------------|----------|--------------------------------------------------------------|
| project_id | str        | Yes      | ID of the project.                                           |
| predict_job_id | str        | Yes      | ID of a predictions deployment used to make the predictions. |

## Usage

1. Connect to a predictions job
2. Generate predictions
3. Format the predictions
4. Upload the formatted predictions to external data storage

## Result

The formatted predictions are uploaded to external data storage. The specific format and
location of the uploaded predictions will depend on the user's implementation of the DAG.
