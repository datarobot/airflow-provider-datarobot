# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datarobot.enums import DataWranglingDialect

from datarobot_provider.example_dags.wrangler_example_recipe import WRANGLER_EXAMPLE_RECIPE
from datarobot_provider.operators.data_prep import CreateWranglingRecipeOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromRecipeOperator
from datarobot_provider.operators.data_registry import UploadDatasetOperator
from datarobot_provider.operators.datarobot import GetOrCreateUseCaseOperator
from datarobot_provider.operators.deployment import DeployRegisteredModelOperator
from datarobot_provider.operators.deployment import ScorePredictionsOperator
from datarobot_provider.operators.monitoring import UpdateDriftTrackingOperator
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor

"""
Example of Aiflow DAG for DataRobot data deployment and prediction generation.
Configurable parameters for this dag:
* model_package_id - The ID of the DataRobot model package (version) to deploy.
* deployment_label - A human readable label of the deployment.
* default_prediction_server_id - an identifier of a prediction server to be used as the default prediction server
  When working with prediction environments, default prediction server Id should not be provided
* target_drift_enabled - if target drift tracking is to be turned on
* feature_drift_enabled - if feature drift tracking is to be turned on
* predictions_dataset_file_path - the path to the dataset to be used for predictions

PREDICTION SERVER IDS:
See https://docs.datarobot.com/en/docs/api/reference/predapi/pred-server-id.html

"""


@dag(
    schedule=None,
    tags=["example", "csv", "predictions", "deployment"],
    params={
        "model_package_id": "",
        "deployment_label": "hospital-readmissions-example-deployment-prediction",
        "default_prediction_server_id": "",
        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "predictions_dataset_file_path": "https://s3.amazonaws.com/datarobot_public_datasets/10k_diabetes.csv",
    },
)
def deployment_prediction_generation():
    # Create a Use Case to keep all subsequent assets. Default name is "Airflow"
    create_use_case = GetOrCreateUseCaseOperator(task_id="create_use_case", set_default=True)

    deploy_registered_model = DeployRegisteredModelOperator(
        task_id="deploy_registered_model",
        model_package_id="{{ params.model_package_id }}",
        deployment_label="{{ params.deployment_label }}",
        extra_params={"default_prediction_server_id": "{{ params.default_prediction_server_id }}"},
    )

    update_drift_tracking = UpdateDriftTrackingOperator(
        task_id="update_drift_tracking",
        deployment_id=str(deploy_registered_model.output),
        target_drift_enabled=bool("{{ params.target_drift_enabled }}"),
        feature_drift_enabled=bool("{{ params.feature_drift_enabled }}"),
    )

    # Upload the data into Data Registry.
    predictions_dataset = UploadDatasetOperator(
        task_id="upload_dataset", file_path="{{ params.predictions_dataset_file_path }}"
    )

    # Define data preparation.
    predictions_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        dataset_id=str(predictions_dataset.output),
        dialect=DataWranglingDialect.SPARK,
        operations=WRANGLER_EXAMPLE_RECIPE,
    )

    # Apply data preparation and save the modified data in the Data Registry.
    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe",
        recipe_id=str(predictions_recipe.output),
        do_snapshot=True,
    )

    deployment_predictions = ScorePredictionsOperator(
        task_id="deployment_predictions",
        deployment_id=str(deploy_registered_model.output),
        score_settings={
            "intake_settings": {
                "type": "dataset",
                "dataset_id": str(publish_recipe.output),
            }
        },
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=str(deployment_predictions.output),
    )

    collect_ops = EmptyOperator(task_id="collect_ops")

    (create_use_case >> [deploy_registered_model, predictions_dataset])
    (deploy_registered_model >> update_drift_tracking)
    (predictions_dataset >> predictions_recipe >> publish_recipe)
    ([update_drift_tracking, publish_recipe] >> collect_ops)
    (collect_ops >> deployment_predictions >> scoring_complete_sensor)


deployment_prediction_generation()
