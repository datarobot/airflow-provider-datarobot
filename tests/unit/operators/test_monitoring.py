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
from datarobot.models import ServiceStats

from datarobot_provider.operators.monitoring import GetServiceStatsOperator
from datarobot_provider.operators.monitoring import _serialize_service_stats


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
    expected_service_stats = _serialize_service_stats(service_stat)

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))
    get_service_stats_mock = mocker.patch.object(
        dr.Deployment, "get_service_stats", return_value=service_stat
    )

    operator = GetServiceStatsOperator(task_id="get_service_stat", deployment_id="deployment-id")
    # service_stats_params = {"feature_drift": {"model_id": "test-model-id"}}
    service_stats_params = {}
    service_stats_result = operator.execute(context={'params': {}})

    assert service_stats_result == expected_service_stats
    get_service_stats_mock.assert_called_with(**service_stats_params)


def test_operator_get_service_stat_with_params(mocker, service_stat_details):
    deployment_id = "deployment-id"
    service_stat = ServiceStats(**service_stat_details)
    expected_service_stats = _serialize_service_stats(service_stat)

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
