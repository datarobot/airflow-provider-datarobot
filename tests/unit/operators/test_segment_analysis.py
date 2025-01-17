# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.segment_analysis import GetSegmentAnalysisSettingsOperator
from datarobot_provider.operators.segment_analysis import UpdateSegmentAnalysisSettingsOperator


@pytest.fixture
def segment_analysis_settings_details():
    return {
        "enabled": True,
        "attributes": ["race"],
    }


def test_operator_get_segment_analysis_settings(mocker, segment_analysis_settings_details):
    deployment_id = "deployment-id"

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_segment_analysis_settings",
        return_value=segment_analysis_settings_details,
    )

    operator = GetSegmentAnalysisSettingsOperator(
        task_id="get_segment_analysis_settings", deployment_id="deployment-id"
    )

    segment_analysis_settings_result = operator.execute(context={"params": {}})

    assert segment_analysis_settings_result == segment_analysis_settings_details


def test_operator_update_segment_analysis_settings(mocker, segment_analysis_settings_details):
    deployment_id = "deployment-id"

    segment_analysis_settings_params = {
        "segment_analysis_enabled": True,
        "segment_analysis_attributes": ["race", "gender"],
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_segment_analysis_settings",
        return_value=segment_analysis_settings_details,
    )

    update_segment_analysis_settings_mock = mocker.patch.object(
        dr.Deployment, "update_segment_analysis_settings"
    )

    operator = UpdateSegmentAnalysisSettingsOperator(
        task_id="update_segment_analysis_settings", deployment_id="deployment-id"
    )

    operator.execute(context={"params": segment_analysis_settings_params})

    update_segment_analysis_settings_mock.assert_called_with(
        segment_analysis_enabled=segment_analysis_settings_params["segment_analysis_enabled"],
        segment_analysis_attributes=segment_analysis_settings_params["segment_analysis_attributes"],
    )


def test_operator_non_need_update_segment_analysis_settings(
    mocker, segment_analysis_settings_details
):
    deployment_id = "deployment-id"

    segment_analysis_settings_params = {
        "segment_analysis_enabled": True,
        "segment_analysis_attributes": ["race"],
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_segment_analysis_settings",
        return_value=segment_analysis_settings_details,
    )

    update_segment_analysis_settings_mock = mocker.patch.object(
        dr.Deployment, "update_segment_analysis_settings"
    )

    operator = UpdateSegmentAnalysisSettingsOperator(
        task_id="update_segment_analysis_settings", deployment_id="deployment-id"
    )

    operator.execute(context={"params": segment_analysis_settings_params})

    update_segment_analysis_settings_mock.assert_not_called()
