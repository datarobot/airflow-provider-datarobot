# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.bias_and_fairness import GetBiasAndFairnessSettingsOperator
from datarobot_provider.operators.bias_and_fairness import UpdateBiasAndFairnessSettingsOperator


@pytest.fixture
def bias_and_fairness_settings_details():
    return {
        'protected_features': ['gender'],
        'preferable_target_value': 'True',
        'fairness_metrics_set': 'equalParity',
        'fairness_threshold': 0.1,
    }


def test_operator_get_bias_and_fairness_settings(mocker, bias_and_fairness_settings_details):
    deployment_id = "deployment-id"

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_bias_and_fairness_settings",
        return_value=bias_and_fairness_settings_details,
    )

    operator = GetBiasAndFairnessSettingsOperator(
        task_id="get_bias_and_fairness_settings", deployment_id="deployment-id"
    )

    bias_and_fairness_settings_result = operator.execute(context={'params': {}})

    assert bias_and_fairness_settings_result == bias_and_fairness_settings_details


def test_operator_update_bias_and_fairness_settings(mocker, bias_and_fairness_settings_details):
    deployment_id = "deployment-id"

    bias_and_fairness_settings_params = {
        'protected_features': ['gender'],
        'preferable_target_value': 'True',
        'fairness_metrics_set': 'equalParity',
        'fairness_threshold': 0.25,
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_bias_and_fairness_settings",
        return_value=bias_and_fairness_settings_details,
    )

    update_bias_and_fairness_settings_mock = mocker.patch.object(
        dr.Deployment, "update_bias_and_fairness_settings"
    )

    operator = UpdateBiasAndFairnessSettingsOperator(
        task_id="update_bias_and_fairness_settings", deployment_id="deployment-id"
    )

    operator.execute(context={'params': bias_and_fairness_settings_params})

    update_bias_and_fairness_settings_mock.assert_called_with(
        protected_features=bias_and_fairness_settings_params['protected_features'],
        fairness_metric_set=bias_and_fairness_settings_params['fairness_metrics_set'],
        fairness_threshold=bias_and_fairness_settings_params['fairness_threshold'],
        preferable_target_value=bias_and_fairness_settings_params['preferable_target_value'],
    )


def test_operator_no_need_update_bias_and_fairness_settings(
    mocker, bias_and_fairness_settings_details
):
    deployment_id = "deployment-id"

    bias_and_fairness_settings_params = {
        'protected_features': ['gender'],
        'preferable_target_value': 'True',
        'fairness_metrics_set': 'equalParity',
        'fairness_threshold': 0.1,
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_bias_and_fairness_settings",
        return_value=bias_and_fairness_settings_details,
    )

    update_bias_and_fairness_settings_mock = mocker.patch.object(
        dr.Deployment, "update_bias_and_fairness_settings"
    )

    operator = UpdateBiasAndFairnessSettingsOperator(
        task_id="update_bias_and_fairness_settings", deployment_id="deployment-id"
    )

    operator.execute(context={'params': bias_and_fairness_settings_params})

    update_bias_and_fairness_settings_mock.assert_not_called()
