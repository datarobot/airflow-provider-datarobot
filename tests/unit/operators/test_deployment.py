# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from unittest.mock import Mock

import datarobot as dr
import pytest
from airflow.exceptions import AirflowFailException

from datarobot_provider.operators.deployment import ActivateDeploymentOperator
from datarobot_provider.operators.deployment import GetDeploymentModelOperator
from datarobot_provider.operators.deployment import GetDeploymentStatusOperator
from datarobot_provider.operators.deployment import ReplaceModelOperator


@pytest.fixture
def validate_replacement_model_passing_details():
    return "passing", "Model can be used to replace the current model of the deployment.", {}


@pytest.fixture
def validate_replacement_model_failed_details():
    return "failing", "Model cannot be used to replace the current model of the deployment.", {}


@pytest.fixture
def deployment_model_details():
    return {
        "id": "64a7f27e8e0fd5cae6282a8d",
        "type": "Light Gradient Boosted Trees Classifier with Early Stopping",
        "target_name": "readmitted",
        "project_id": "64a7f21f1110f0df910ddb4f",
        "target_type": "Binary",
        "project_name": "Airflow-Demo",
        "unsupervised_mode": False,
        "unstructured_model_kind": False,
        "build_environment_type": "DataRobot",
        "deployed_at": "2023-07-19T13:54:42.927000Z",
        "has_decision_flow": False,
        "is_deprecated": False,
    }


@pytest.fixture
def deployment_mock_obj(deployment_model_details):
    deployment_mock = Mock()
    deployment_mock.model = deployment_model_details
    return deployment_mock


def test_operator_get_deployment_model(mocker, deployment_mock_obj, deployment_model_details):
    deployment_id = "deployment-id"
    dr.Deployment(deployment_id)
    mocker.patch.object(dr.Deployment, "get", return_value=deployment_mock_obj)

    operator = GetDeploymentModelOperator(
        task_id="get_deployment_model", deployment_id="deployment-id"
    )

    get_deployment_model_operator_result = operator.execute(context={"params": {}})

    assert get_deployment_model_operator_result == deployment_model_details


def test_operator_replace_deployment_model(mocker, validate_replacement_model_passing_details):
    deployment_id = "deployment-id"
    new_registered_model_version_id = "new-registered-model-version-id"

    get_deployment_mock = mocker.patch.object(
        dr.Deployment, "get", return_value=dr.Deployment(deployment_id)
    )

    validate_replacement_model_mock = mocker.patch.object(
        dr.Deployment,
        "validate_replacement_model",
        return_value=validate_replacement_model_passing_details,
    )

    replace_model_mock = mocker.patch.object(dr.Deployment, "perform_model_replace")

    operator = ReplaceModelOperator(
        task_id="replace_deployment_model",
        deployment_id=deployment_id,
        new_registered_model_version_id=new_registered_model_version_id,
        reason="ACCURACY",
        max_wait_sec=3600,
    )

    operator.execute(context={"params": {}})

    get_deployment_mock.assert_called_with(
        deployment_id=deployment_id,
    )

    validate_replacement_model_mock.assert_called_with(
        new_registered_model_version_id=new_registered_model_version_id,
    )

    replace_model_mock.assert_called_with(
        new_registered_model_version_id=new_registered_model_version_id,
        reason="ACCURACY",
        max_wait=3600,
    )


def test_operator_replace_deployment_model_failed(
    mocker, validate_replacement_model_failed_details
):
    deployment_id = "deployment-id"
    new_registered_model_version_id = "new-registered-model-version-id"

    get_deployment_mock = mocker.patch.object(
        dr.Deployment, "get", return_value=dr.Deployment(deployment_id)
    )

    validate_replacement_model_mock = mocker.patch.object(
        dr.Deployment,
        "validate_replacement_model",
        return_value=validate_replacement_model_failed_details,
    )

    replace_model_mock = mocker.patch.object(dr.Deployment, "perform_model_replace")

    operator = ReplaceModelOperator(
        task_id="replace_deployment_model",
        deployment_id=deployment_id,
        new_registered_model_version_id=new_registered_model_version_id,
        reason="ACCURACY",
        max_wait_sec=3600,
    )

    with pytest.raises(AirflowFailException):
        operator.execute(context={"params": {}})

    get_deployment_mock.assert_called_with(
        deployment_id=deployment_id,
    )

    validate_replacement_model_mock.assert_called_with(
        new_registered_model_version_id=new_registered_model_version_id,
    )

    replace_model_mock.assert_not_called()


def test_operator_activate_deployment(mocker):
    deployment_id = "deployment-id"
    deployment = dr.Deployment(deployment_id)

    deployment.status = "active"

    get_deployment_mock = mocker.patch.object(dr.Deployment, "get", return_value=deployment)

    activate_deployment_mock = mocker.patch.object(
        dr.Deployment,
        "activate",
        return_value=None,
    )

    deactivate_deployment_mock = mocker.patch.object(
        dr.Deployment,
        "deactivate",
        return_value=None,
    )

    operator = ActivateDeploymentOperator(
        task_id="replace_deployment_model",
        deployment_id=deployment_id,
        activate=True,
        max_wait_sec=1000,
    )

    operator_result = operator.execute(context={"params": {}})
    get_deployment_mock.assert_called_with(deployment_id)
    activate_deployment_mock.assert_called_with(max_wait=1000)
    deactivate_deployment_mock.assert_not_called()

    assert operator_result == "active"


def test_operator_deactivate_deployment(mocker):
    deployment_id = "deployment-id"
    deployment = dr.Deployment(deployment_id)

    deployment.status = "inactive"

    get_deployment_mock = mocker.patch.object(dr.Deployment, "get", return_value=deployment)

    activate_deployment_mock = mocker.patch.object(
        dr.Deployment,
        "activate",
        return_value=None,
    )

    deactivate_deployment_mock = mocker.patch.object(
        dr.Deployment,
        "deactivate",
        return_value=None,
    )

    operator = ActivateDeploymentOperator(
        task_id="activate_deployment",
        deployment_id=deployment_id,
        activate=False,
        max_wait_sec=1000,
    )

    operator_result = operator.execute(context={"params": {}})
    get_deployment_mock.assert_called_with(deployment_id)
    deactivate_deployment_mock.assert_called_with(max_wait=1000)
    activate_deployment_mock.assert_not_called()

    assert operator_result == "inactive"


def test_operator_activate_deployment_not_provided(mocker):
    deployment_id = None

    operator = ActivateDeploymentOperator(
        task_id="activate_deployment",
        deployment_id=deployment_id,
        activate=True,
        max_wait_sec=1000,
    )

    with pytest.raises(ValueError):
        operator.execute(operator.execute(context={"params": {}}))


def test_operator_get_deployment_status(mocker):
    deployment_id = "deployment-id"
    deployment = dr.Deployment(deployment_id)

    deployment.status = "inactive"

    get_deployment_mock = mocker.patch.object(dr.Deployment, "get", return_value=deployment)

    operator = GetDeploymentStatusOperator(
        task_id="get_deployment_status",
        deployment_id=deployment_id,
    )

    operator_result = operator.execute(context={"params": {}})
    get_deployment_mock.assert_called_with(deployment_id)

    assert operator_result == "inactive"


def test_operator_get_deployment_status_not_provided(mocker):
    deployment_id = None

    operator = GetDeploymentStatusOperator(
        task_id="get_deployment_status",
        deployment_id=deployment_id,
    )

    with pytest.raises(ValueError):
        operator.execute(operator.execute(context={"params": {}}))
