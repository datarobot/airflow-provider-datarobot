# Dataprep and Autopilot

`path: datarobot_provider/examples/dataprep_wrangler_autopilot.py`

The `dataprep_wrangler_autopilot` DAG demonstrates how to use the DataRobot provider for Apache Airflow to create a project, train a model, and register it to DataRobot MLOps.
The initial demo flow utilizes a public .csv file with hospital readmissions data.

## Requirements

* ENABLE_DATA_REGISTRY_WRANGLING
* ENABLE_MLOPS

## Input Parameters

<!-- Do we need anything here? Or just N/A? -->

## Usage

1. Create or reuse a use case
2. Upload a dataset
3. Create a wrangler recipe
4. Publish and run the wrangler recipe
5. Create a new project/experiment in the use case
6. Run autopilot in quick mode
7. Wait for autopilot to complete before continuing
8. Select the top model
9. Register the model for deployment

## Result

You now have a project and model registered with DataRobot MLOps.

## Troubleshooting

_N/A_
