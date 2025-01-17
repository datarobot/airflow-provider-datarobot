# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task

from datarobot_provider.operators.monitoring import GetMonitoringSettingsOperator
from datarobot_provider.operators.monitoring import UpdateMonitoringSettingsOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
    # Default json config example:
    params={
        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "association_id_column": ["id"],
        "required_association_id": False,
        "predictions_data_collection_enabled": False,
    },
)
def deployment_monitoring_settings(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")
    get_monitoring_settings_before_op = GetMonitoringSettingsOperator(
        task_id="get_monitoring_settings_before",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    update_monitoring_settings_op = UpdateMonitoringSettingsOperator(
        task_id="update_monitoring_settings",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    get_monitoring_settings_after_op = GetMonitoringSettingsOperator(
        task_id="get_monitoring_settings_after",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    @task(task_id="example_processing_python")
    def monitoring_settings_processing(
        model_monitoring_settings_before, model_monitoring_settings_after
    ):
        """Example of custom logic based on monitoring_settings from the deployment."""

        # Put your service stat processing logic here:

        settings_changed = {
            setting: model_monitoring_settings_after[setting]
            for setting in model_monitoring_settings_after
            if model_monitoring_settings_after[setting] != model_monitoring_settings_before[setting]
        }

        return settings_changed

    example_service_stat_processing = monitoring_settings_processing(
        model_monitoring_settings_before=get_monitoring_settings_before_op.output,
        model_monitoring_settings_after=get_monitoring_settings_after_op.output,
    )

    (
        get_monitoring_settings_before_op
        >> update_monitoring_settings_op
        >> get_monitoring_settings_after_op
        >> example_service_stat_processing
    )


deployment_monitoring_settings_dag = deployment_monitoring_settings()

if __name__ == "__main__":
    deployment_monitoring_settings_dag.test()
