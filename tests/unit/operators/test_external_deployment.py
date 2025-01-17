# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import pytest
from datarobot.enums import DEPLOYMENT_IMPORTANCE

from datarobot_provider.operators.model_package import DeployModelPackageOperator


@pytest.fixture
def external_deployment_params():
    return {
        'model_package_id': 'test-model-package-id',
        'deployment_name': 'Test Deployment',
        'description': 'Test Description',
        'user_provided_id': 'test-user-provided-id',
        'importance': DEPLOYMENT_IMPORTANCE.LOW,
        'additional_metadata': {'key': 'test-key', 'value': 'test-value'},
    }


def test_operator_create_external_deployment_op(mocker, external_deployment_params):
    external_deployment_id = 'test-external-deployment-id'
    prediction_environment_id = 'test-prediction-environment-id'
    create_external_deployment_mock = mocker.patch.object(
        DeployModelPackageOperator,
        '_create_from_model_package',
        return_value=external_deployment_id,
    )

    operator = DeployModelPackageOperator(
        task_id='create_external_deployment',
        deployment_name=external_deployment_params['deployment_name'],
        description=external_deployment_params['description'],
        model_package_id=external_deployment_params['model_package_id'],
        prediction_environment_id=prediction_environment_id,
        importance=external_deployment_params['importance'],
        user_provided_id=external_deployment_params['user_provided_id'],
        additional_metadata=external_deployment_params['additional_metadata'],
        max_wait_sec=1000,
    )

    operator_result = operator.execute(context={'params': external_deployment_params})

    create_external_deployment_mock.assert_called_with(
        model_package_id=external_deployment_params['model_package_id'],
        deployment_name=external_deployment_params['deployment_name'],
        description=external_deployment_params['description'],
        default_prediction_server_id=None,
        prediction_environment_id=prediction_environment_id,
        importance=DEPLOYMENT_IMPORTANCE.LOW,
        user_provided_id=external_deployment_params['user_provided_id'],
        additional_metadata=external_deployment_params['additional_metadata'],
        max_wait_sec=1000,
    )

    assert operator_result == external_deployment_id


def test_operator_create_deployment_op(mocker, external_deployment_params):
    default_prediction_server_id = 'test-default-prediction-server-id'
    external_deployment_id = 'test-external-deployment-id'
    create_external_deployment_mock = mocker.patch.object(
        DeployModelPackageOperator,
        '_create_from_model_package',
        return_value=external_deployment_id,
    )

    operator = DeployModelPackageOperator(
        task_id='create_external_deployment',
        deployment_name=external_deployment_params['deployment_name'],
        description=external_deployment_params['description'],
        model_package_id=external_deployment_params['model_package_id'],
        default_prediction_server_id=default_prediction_server_id,
        importance=external_deployment_params['importance'],
        user_provided_id=external_deployment_params['user_provided_id'],
        additional_metadata=external_deployment_params['additional_metadata'],
        max_wait_sec=1000,
    )

    operator_result = operator.execute(context={'params': external_deployment_params})

    create_external_deployment_mock.assert_called_with(
        model_package_id=external_deployment_params['model_package_id'],
        deployment_name=external_deployment_params['deployment_name'],
        description=external_deployment_params['description'],
        default_prediction_server_id=default_prediction_server_id,
        prediction_environment_id=None,
        importance=DEPLOYMENT_IMPORTANCE.LOW,
        user_provided_id=external_deployment_params['user_provided_id'],
        additional_metadata=external_deployment_params['additional_metadata'],
        max_wait_sec=1000,
    )

    assert operator_result == external_deployment_id


def test_operator_create_external_deployment_invalid_params_op(mocker, external_deployment_params):
    default_prediction_server_id = 'test-default-prediction-server-id'
    prediction_environment_id = 'test-prediction-environment-id'

    operator = DeployModelPackageOperator(
        task_id='create_external_deployment',
        deployment_name=external_deployment_params['deployment_name'],
        description=external_deployment_params['description'],
        model_package_id=external_deployment_params['model_package_id'],
        default_prediction_server_id=default_prediction_server_id,
        prediction_environment_id=prediction_environment_id,
        importance=external_deployment_params['importance'],
        user_provided_id=external_deployment_params['user_provided_id'],
        additional_metadata=external_deployment_params['additional_metadata'],
        max_wait_sec=1000,
    )

    with pytest.raises(ValueError):
        operator.execute(context={'params': external_deployment_params})
