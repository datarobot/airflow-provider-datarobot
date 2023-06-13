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

from datarobot_provider.operators.monitoring import (
    UpdateMonitoringSettingsOperator,
    GetMonitoringSettingsOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'mlops'],
    # Default json config example:
    params={
        "target_drift_enabled": False,
        "feature_drift_enabled": True,
        "association_id_column": ["id"],
        "required_association_id": False,
        "enable_challenger_analysis": False,
    },
)
def deployment_monitoring_settings(deployment_id="646fcfe9b01540a797f224b3"):
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

        print(f"settings:{str(model_monitoring_settings_before)}")
        print(f"settings:{str(model_monitoring_settings_after)}")

        settings_changed = {
            setting: model_monitoring_settings_after[setting]
            for setting in model_monitoring_settings_after
            if model_monitoring_settings_after[setting] != model_monitoring_settings_before[setting]
        }

        print(f"settings_changed:{str(settings_changed)}")

        total_predictions = 10
        return total_predictions

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
