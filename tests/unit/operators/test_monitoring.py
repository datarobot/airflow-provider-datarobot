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
from datarobot.models.deployment import Accuracy
from datarobot.models.deployment import ServiceStats

from datarobot_provider.operators.monitoring import GetAccuracyOperator
from datarobot_provider.operators.monitoring import GetMonitoringSettingsOperator
from datarobot_provider.operators.monitoring import GetServiceStatsOperator
from datarobot_provider.operators.monitoring import UpdateDriftTrackingOperator
from datarobot_provider.operators.monitoring import UpdateMonitoringSettingsOperator
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
            "totalPredictions": 1000,
            "totalRequests": 10,
            "slowRequests": 5,
            "executionTime": 500.0,
            "responseTime": 1000.0,
            "userErrorRate": 0.0,
            "serverErrorRate": 0.0,
            "numConsumers": 1,
            "cacheHitRatio": 1.0,
            "medianLoad": 0.0,
            "peakLoad": 1,
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
    service_stats_result = operator.execute(context={"params": {}})

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

    service_stats_result = operator.execute(context={"params": service_stats_params})

    assert service_stats_result == expected_service_stats
    get_service_stats_mock.assert_called_with(**service_stats_params["service_stats"])


@pytest.fixture
def accuracy_details():
    return {
        "model_id": "test-model-id",
        "period": {
            "start": datetime.fromisoformat("2023-05-01"),
            "end": datetime.fromisoformat("2023-05-07"),
        },
        "metrics": {
            "RMSE": {
                "value": 212.951709001837,
                "baseline_value": 1968.4643697912,
                "percent_change": 89.18,
            },
            "MAE": {
                "value": 212.511998013901,
                "baseline_value": 1366.90728550895,
                "percent_change": 84.45,
            },
            "Gamma Deviance": {
                "value": 0.000417428054515133,
                "baseline_value": 0.0131687235807189,
                "percent_change": 96.83,
            },
            "Tweedie Deviance": {
                "value": 0.0425429308927789,
                "baseline_value": 1.57434163465787,
                "percent_change": 97.3,
            },
            "R Squared": {
                "value": 0.8695494587538976,
                "baseline_value": 0.9366663850412537,
                "percent_change": -7.17,
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
    accuracy_result = operator.execute(context={"params": {}})

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

    accuracy_result = operator.execute(context={"params": accuracy_params})

    assert accuracy_result == expected_accuracy
    get_accuracy_mock.assert_called_with(**accuracy_params["accuracy"])


@pytest.fixture
def monitoring_settings_details():
    return {
        "drift_tracking_settings": {
            "target_drift": {"enabled": False},
            "feature_drift": {"enabled": False},
        },
        "association_id_settings": {
            "column_names": ["id"],
            "required_in_prediction_requests": False,
        },
        "predictions_data_collection_settings": {"enabled": False},
    }


def test_operator_get_monitoring_settings(mocker, monitoring_settings_details):
    deployment_id = "deployment-id"

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_drift_tracking_settings",
        return_value=monitoring_settings_details["drift_tracking_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_association_id_settings",
        return_value=monitoring_settings_details["association_id_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_predictions_data_collection_settings",
        return_value=monitoring_settings_details["predictions_data_collection_settings"],
    )

    operator = GetMonitoringSettingsOperator(
        task_id="get_monitoring_settings", deployment_id="deployment-id"
    )

    monitoring_settings_result = operator.execute(context={"params": {}})

    assert monitoring_settings_result == monitoring_settings_details


def test_operator_update_monitoring_settings(mocker, monitoring_settings_details):
    deployment_id = "deployment-id"

    monitoring_settings_params = {
        "target_drift_enabled": True,
        "feature_drift_enabled": True,
        "association_id_column": ["association_id"],
        "required_association_id": True,
        "predictions_data_collection_enabled": True,
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_drift_tracking_settings",
        return_value=monitoring_settings_details["drift_tracking_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_association_id_settings",
        return_value=monitoring_settings_details["association_id_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_predictions_data_collection_settings",
        return_value=monitoring_settings_details["predictions_data_collection_settings"],
    )

    update_drift_tracking_settings_mock = mocker.patch.object(
        dr.Deployment, "update_drift_tracking_settings"
    )

    update_association_id_settings_mock = mocker.patch.object(
        dr.Deployment, "update_association_id_settings"
    )

    update_predictions_data_collection_settings_mock = mocker.patch.object(
        dr.Deployment, "update_predictions_data_collection_settings", return_value=None
    )

    operator = UpdateMonitoringSettingsOperator(
        task_id="update_monitoring_settings", deployment_id="deployment-id"
    )

    operator.execute(context={"params": monitoring_settings_params})

    update_drift_tracking_settings_mock.assert_called_with(
        target_drift_enabled=monitoring_settings_params["target_drift_enabled"],
        feature_drift_enabled=monitoring_settings_params["feature_drift_enabled"],
    )

    update_association_id_settings_mock.assert_called_with(
        column_names=monitoring_settings_params["association_id_column"],
        required_in_prediction_requests=monitoring_settings_params["required_association_id"],
    )

    update_predictions_data_collection_settings_mock.assert_called_with(
        enabled=monitoring_settings_params["predictions_data_collection_enabled"]
    )


def test_operator_no_need_update_monitoring_settings(mocker, monitoring_settings_details):
    deployment_id = "deployment-id"

    monitoring_settings_params = {
        "target_drift_enabled": False,
        "feature_drift_enabled": False,
        "association_id_column": ["id"],
        "required_association_id": False,
        "predictions_data_collection_enabled": False,
    }

    mocker.patch.object(dr.Deployment, "get", return_value=dr.Deployment(deployment_id))

    mocker.patch.object(
        dr.Deployment,
        "get_drift_tracking_settings",
        return_value=monitoring_settings_details["drift_tracking_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_association_id_settings",
        return_value=monitoring_settings_details["association_id_settings"],
    )

    mocker.patch.object(
        dr.Deployment,
        "get_predictions_data_collection_settings",
        return_value=monitoring_settings_details["predictions_data_collection_settings"],
    )

    update_drift_tracking_settings_mock = mocker.patch.object(
        dr.Deployment, "update_drift_tracking_settings"
    )

    update_association_id_settings_mock = mocker.patch.object(
        dr.Deployment, "update_association_id_settings"
    )

    update_predictions_data_collection_settings_mock = mocker.patch.object(
        dr.Deployment, "update_predictions_data_collection_settings", return_value=None
    )

    operator = UpdateMonitoringSettingsOperator(
        task_id="update_monitoring_settings", deployment_id="deployment-id"
    )

    operator.execute(context={"params": monitoring_settings_params})

    update_drift_tracking_settings_mock.assert_not_called()
    update_association_id_settings_mock.assert_not_called()
    update_predictions_data_collection_settings_mock.assert_not_called()


def test_update_drift_settings_execute_success(mocker):
    """Test that execute() calls update_drift_tracking_settings with the correct parameters and returns the deployment_id."""
    dummy_deployment = dr.Deployment("test_deployment_id")
    update_patch = mocker.patch.object(dummy_deployment, "update_drift_tracking_settings")
    mocker.patch.object(dr.Deployment, "get", return_value=dummy_deployment)

    op = UpdateDriftTrackingOperator(
        task_id="test",
        deployment_id="test_deployment_id",
        target_drift_enabled=True,
        feature_drift_enabled=False,
    )

    context = {}
    result = op.execute(context)

    update_patch.assert_called_once_with(target_drift_enabled=True, feature_drift_enabled=False)
    assert result == "test_deployment_id"


def test_update_drift_settings_validate_missing_deployment_id():
    """Test that validate() raises a ValueError when deployment_id is missing."""
    op = UpdateDriftTrackingOperator(
        task_id="test",
        deployment_id="",
        target_drift_enabled=True,
        feature_drift_enabled=True,
    )
    with pytest.raises(ValueError, match="deployment_id must be provided."):
        op.validate()
