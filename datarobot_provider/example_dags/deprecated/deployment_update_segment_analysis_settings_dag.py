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

from datarobot_provider.operators.bias_and_fairness import GetBiasAndFairnessSettingsOperator
from datarobot_provider.operators.bias_and_fairness import UpdateBiasAndFairnessSettingsOperator
from datarobot_provider.operators.segment_analysis import GetSegmentAnalysisSettingsOperator
from datarobot_provider.operators.segment_analysis import UpdateSegmentAnalysisSettingsOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
    # Default json config example:
    params={
        "segment_analysis_enabled": True,
        "segment_analysis_attributes": ["race", "gender"],
        "protected_features": ["gender"],
        "preferable_target_value": "True",
        "fairness_metric_set": "equalParity",
        "fairness_threshold": 0.1,
    },
)
def deployment_segment_analysis_settings(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")
    get_segment_analysis_settings_before_op = GetSegmentAnalysisSettingsOperator(
        task_id="get_segment_analysis_settings_before",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    get_bias_and_fairness_settings_before_op = GetBiasAndFairnessSettingsOperator(
        task_id="get_bias_and_fairness_settings_before",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    update_segment_analysis_settings_op = UpdateSegmentAnalysisSettingsOperator(
        task_id="update_segment_analysis_settings",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    update_bias_and_fairness_settings_op = UpdateBiasAndFairnessSettingsOperator(
        task_id="update_bias_and_fairness_settings",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    get_segment_analysis_settings_after_op = GetSegmentAnalysisSettingsOperator(
        task_id="get_segment_analysis_after",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    get_bias_and_fairness_settings_after_op = GetBiasAndFairnessSettingsOperator(
        task_id="get_bias_and_fairness_settings_after",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
    )

    @task(task_id="example_processing_python")
    def settings_processing(
        segment_analysis_settings_before,
        segment_analysis_settings_after,
        bias_and_fairness_settings_before,
        bias_and_fairness_settings_after,
    ):
        """Example of custom logic based on segment analysis settings from the deployment."""

        # Put your service stat processing logic here:

        segment_analysis_settings_changed = {
            setting: segment_analysis_settings_after[setting]
            for setting in segment_analysis_settings_after
            if segment_analysis_settings_after[setting] != segment_analysis_settings_before[setting]
        }

        bias_and_fairness_settings_changed = {
            setting: bias_and_fairness_settings_after[setting]
            for setting in bias_and_fairness_settings_after
            if bias_and_fairness_settings_after[setting]
            != bias_and_fairness_settings_before[setting]
        }

        return segment_analysis_settings_changed, bias_and_fairness_settings_changed

    example_service_stat_processing = settings_processing(
        segment_analysis_settings_before=get_segment_analysis_settings_before_op.output,
        segment_analysis_settings_after=get_segment_analysis_settings_after_op.output,
        bias_and_fairness_settings_before=get_bias_and_fairness_settings_before_op.output,
        bias_and_fairness_settings_after=get_bias_and_fairness_settings_after_op.output,
    )

    (
        get_segment_analysis_settings_before_op
        >> get_bias_and_fairness_settings_before_op
        >> update_segment_analysis_settings_op
        >> update_bias_and_fairness_settings_op
        >> get_segment_analysis_settings_after_op
        >> get_bias_and_fairness_settings_after_op
        >> example_service_stat_processing
    )


deployment_segment_analysis_dag = deployment_segment_analysis_settings()

if __name__ == "__main__":
    deployment_segment_analysis_dag.test()
