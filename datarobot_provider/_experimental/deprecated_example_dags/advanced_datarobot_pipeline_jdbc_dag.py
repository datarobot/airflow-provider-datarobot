# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task

from datarobot_provider.operators.bias_and_fairness import UpdateBiasAndFairnessSettingsOperator
from datarobot_provider.operators.data_registry import CreateDatasetFromDataStoreOperator
from datarobot_provider.operators.data_registry import GetDataStoreOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator
from datarobot_provider.operators.datarobot import TrainModelsOperator
from datarobot_provider.operators.deployment import DeployRecommendedModelOperator
from datarobot_provider.operators.deployment import GetFeatureDriftOperator
from datarobot_provider.operators.deployment import GetTargetDriftOperator
from datarobot_provider.operators.deployment import ScorePredictionsOperator
from datarobot_provider.operators.monitoring import GetAccuracyOperator
from datarobot_provider.operators.monitoring import GetServiceStatsOperator
from datarobot_provider.operators.monitoring import UpdateMonitoringSettingsOperator
from datarobot_provider.operators.monitoring_job import BatchMonitoringOperator
from datarobot_provider.operators.segment_analysis import UpdateSegmentAnalysisSettingsOperator
from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor
from datarobot_provider.sensors.monitoring_job import MonitoringJobCompleteSensor


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "end-to-end", "pipline"],
    params={
        "data_connection": "Demo Connection",
        "dataset_name": "Demo-Airflow-training-dataset",
        "table_schema": "DEMO_SCHEMA",
        "table_name": "DEMO-TRAINING-DATASET",
        "persist_data_after_ingestion": False,
        "do_snapshot": False,
        "project_name": "Airflow - Demo",
        "unsupervised_mode": False,
        "use_feature_discovery": False,
        "autopilot_settings": {"target": "readmitted", "mode": "quick", "max_wait": 3600},
        "deployment_label": "Deployment from Airflow - Demo",
        "deployment_description": "Airflow - Demo Deployment",
        "score_settings": {
            "intake_settings": {
                "type": "jdbc",
                "table": "DEMO-SCORING-DATASET",
                "schema": "DEMO_SCHEMA",
            },
            "output_settings": {
                "type": "jdbc",
                "schema": "DEMO_SCHEMA",
                "table": "DEMO-SCORING-OUTPUT-DATASET",
                "statement_type": "insert",
                "create_table_if_not_exists": True,
            },
            # "passthrough_columns": ['column1', 'column2'],
            "passthrough_columns_set": "all",
        },
        "segment_analysis_enabled": True,
        "segment_analysis_attributes": ["race", "gender"],
        "protected_features": ["gender"],
        "preferable_target_value": "True",
        "fairness_metric_set": "equalParity",
        "fairness_threshold": 0.1,
        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "association_id_column": ["rowID"],
        "required_association_id": True,
        "predictions_data_collection_enabled": True,
        "monitoring_settings": {
            "intake_settings": {
                "type": "jdbc",
                "table": "DEMO-ACTUALS-DATASET",  # table with actuals
                "schema": "DEMO_SCHEMA",
            },
            "monitoring_columns": {
                "association_id_column": "rowID",
                "actuals_value_column": "ACTUALS",
            },
        },
    },
)
def advanced_datarobot_pipeline_jdbc():
    get_jdbc_connection_op = GetDataStoreOperator(task_id="get_jdbc_connection")

    dataset_connect_op = CreateDatasetFromDataStoreOperator(
        task_id="create_dataset_jdbc",
        data_store_id=get_jdbc_connection_op.output,
    )

    create_project_op = CreateProjectOperator(
        task_id="create_project",
        dataset_id=dataset_connect_op.output,
    )

    train_models_op = TrainModelsOperator(
        task_id="train_models",
        project_id=create_project_op.output,
    )

    autopilot_complete_sensor = AutopilotCompleteSensor(
        task_id="check_autopilot_complete",
        project_id=create_project_op.output,
    )

    deploy_model_op = DeployRecommendedModelOperator(
        task_id="deploy_recommended_model",
        project_id=create_project_op.output,
    )

    update_segment_analysis_settings_op = UpdateSegmentAnalysisSettingsOperator(
        task_id="update_segment_analysis_settings",
        deployment_id=deploy_model_op.output,
    )

    update_bias_and_fairness_settings_op = UpdateBiasAndFairnessSettingsOperator(
        task_id="update_bias_and_fairness_settings",
        deployment_id=deploy_model_op.output,
    )

    update_monitoring_settings_op = UpdateMonitoringSettingsOperator(
        task_id="update_monitoring_settings",
        deployment_id=deploy_model_op.output,
    )

    score_predictions_op = ScorePredictionsOperator(
        task_id="score_predictions_jdbc",
        deployment_id=deploy_model_op.output,
        intake_datastore_id=get_jdbc_connection_op.output,
        output_datastore_id=get_jdbc_connection_op.output,
    )

    scoring_complete_sensor = ScoringCompleteSensor(
        task_id="check_scoring_complete",
        job_id=score_predictions_op.output,
    )

    service_stats_op = GetServiceStatsOperator(
        task_id="get_service_stats",
        deployment_id=deploy_model_op.output,
    )

    target_drift_op = GetTargetDriftOperator(
        task_id="target_drift",
        deployment_id=deploy_model_op.output,
    )

    feature_drift_op = GetFeatureDriftOperator(
        task_id="feature_drift",
        deployment_id=deploy_model_op.output,
    )

    batch_monitoring_op = BatchMonitoringOperator(
        task_id="batch_monitoring",
        deployment_id=deploy_model_op.output,
        datastore_id=get_jdbc_connection_op.output,
    )

    batch_monitoring_complete_sensor = MonitoringJobCompleteSensor(
        task_id="check_monitoring_job_complete",
        job_id=batch_monitoring_op.output,
        poke_interval=5,
        mode="reschedule",
        timeout=3600,
    )

    get_accuracy_op = GetAccuracyOperator(
        task_id="get_accuracy", deployment_id=deploy_model_op.output
    )

    @task(task_id="example_processing_python")
    def service_stat_processing(model_service_stat, feature_drift, target_drift, accuracy):
        """Example of custom logic based on service stats from the deployment."""

        # Put your service stat processing logic here:
        current_model_id = model_service_stat["model_id"]
        total_predictions = model_service_stat["metrics"]["totalPredictions"]
        # example of custom logic:
        return {"model_id": current_model_id, "alive": total_predictions > 0}

    example_service_stat_processing = service_stat_processing(
        model_service_stat=service_stats_op.output,
        feature_drift=target_drift_op.output,
        target_drift=feature_drift_op.output,
        accuracy=get_accuracy_op.output,
    )

    (
        get_jdbc_connection_op
        >> dataset_connect_op
        >> create_project_op
        >> train_models_op
        >> autopilot_complete_sensor
        >> deploy_model_op
        >> update_monitoring_settings_op
        >> update_segment_analysis_settings_op
        >> update_bias_and_fairness_settings_op
        >> score_predictions_op
        >> scoring_complete_sensor
        >> batch_monitoring_op
        >> batch_monitoring_complete_sensor
        >> (get_accuracy_op, target_drift_op, feature_drift_op, service_stats_op)
        >> example_service_stat_processing
    )


advanced_datarobot_pipeline_jdbc_dag = advanced_datarobot_pipeline_jdbc()
if __name__ == "__main__":
    advanced_datarobot_pipeline_jdbc_dag.test()
