# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

import datarobot as dr
import pytest
from datarobot.models import Accuracy
from datarobot.models import ServiceStats

from datarobot_provider.operators.monitoring import GetAccuracyOperator
from datarobot_provider.operators.monitoring import GetServiceStatsOperator
from datarobot_provider.operators.monitoring import _serialize_metrics


@pytest.fixture
def service_stat_details():
    return {
        "model_id": "test-model-id",
        "period": {
            "start": datetime.fromisoformat("2023-01-01"),
            "end": datetime.fromisoformat("2023-01-07"),
        },
        "metrics": {
            'totalPredictions': 1000,
            'totalRequests': 10,
            'slowRequests': 5,
            'executionTime': 500.0,
            'responseTime': 1000.0,
            'userErrorRate': 0.0,
            'serverErrorRate': 0.0,
            'numConsumers': 1,
            'cacheHitRatio': 1.0,
            'medianLoad': 0.0,
            'peakLoad': 1,
        },
    }


def test_operator_get_service_stat(mocker, service_stat_details):
    deployment_id = "deployment-id"
    service_stat = ServiceStats(**service_stat_details)
    expected_service_stats = _serialize_metrics(service_stat)

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_service_stats_mock = mocker.patch.object(
        dr.Deployment, "get_service_stats", return_value=service_stat
    )

    operator = GetServiceStatsOperator(task_id="get_service_stat", deployment_id="deployment-id")
    service_stats_params = {}
    service_stats_result = operator.execute(context={'params': {}})

    assert service_stats_result == expected_service_stats
    get_service_stats_mock.assert_called_with(**service_stats_params)


def test_operator_get_service_stat_with_params(mocker, service_stat_details):
    deployment_id = "deployment-id"
    service_stat = ServiceStats(**service_stat_details)
    expected_service_stats = _serialize_metrics(service_stat)

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_service_stats_mock = mocker.patch.object(
        dr.Deployment, "get_service_stats", return_value=service_stat
    )

    operator = GetServiceStatsOperator(task_id="get_service_stat", deployment_id="deployment-id")

    service_stats_params = {
        "service_stats": {
            "model_id": "test-model-id",
            "start_time": datetime.fromisoformat("2023-01-01"),
            "end_time": datetime.fromisoformat("2023-01-07"),
        }
    }

    service_stats_result = operator.execute(context={'params': service_stats_params})

    assert service_stats_result == expected_service_stats
    get_service_stats_mock.assert_called_with(**service_stats_params["service_stats"])


@pytest.fixture
def accuracy_details():
    return {
        'model_id': "test-model-id",
        'period': {
            'start': datetime.fromisoformat("2023-05-01"),
            'end': datetime.fromisoformat("2023-05-07"),
        },
        'metrics': {
            'RMSE': {
                'value': 212.951709001837,
                'baseline_value': 1968.4643697912,
                'percent_change': 89.18,
            },
            'MAE': {
                'value': 212.511998013901,
                'baseline_value': 1366.90728550895,
                'percent_change': 84.45,
            },
            'Gamma Deviance': {
                'value': 0.000417428054515133,
                'baseline_value': 0.0131687235807189,
                'percent_change': 96.83,
            },
            'Tweedie Deviance': {
                'value': 0.0425429308927789,
                'baseline_value': 1.57434163465787,
                'percent_change': 97.3,
            },
            'R Squared': {
                'value': 0.8695494587538976,
                'baseline_value': 0.9366663850412537,
                'percent_change': -7.17,
            },
        },
    }


def test_operator_get_accuracy(mocker, accuracy_details):
    deployment_id = "deployment-id"
    accuracy = Accuracy(**accuracy_details)
    expected_accuracy = _serialize_metrics(accuracy)

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_accuracy_mock = mocker.patch.object(dr.Deployment, "get_accuracy", return_value=accuracy)

    operator = GetAccuracyOperator(task_id="get_accuracy", deployment_id="deployment-id")
    accuracy_params = {}
    accuracy_result = operator.execute(context={'params': {}})

    assert accuracy_result == expected_accuracy
    get_accuracy_mock.assert_called_with(**accuracy_params)


def test_operator_get_accuracy_with_params(mocker, accuracy_details):
    deployment_id = "deployment-id"
    accuracy = Accuracy(**accuracy_details)
    expected_accuracy = _serialize_metrics(accuracy)

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_accuracy_mock = mocker.patch.object(dr.Deployment, "get_accuracy", return_value=accuracy)

    operator = GetAccuracyOperator(task_id="get_accuracy", deployment_id="deployment-id")

    accuracy_params = {
        "accuracy": {
            "model_id": "test-model-id",
            "start_time": datetime.fromisoformat("2023-05-01"),
            "end_time": datetime.fromisoformat("2023-05-07"),
        }
    }

    accuracy_result = operator.execute(context={'params': accuracy_params})

    assert accuracy_result == expected_accuracy
    get_accuracy_mock.assert_called_with(**accuracy_params["accuracy"])
