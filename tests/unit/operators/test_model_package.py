# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import pytest
from datarobot import TARGET_TYPE

from datarobot_provider.operators.model_package import CreateExternalModelPackageOperator


@pytest.fixture
def external_model_params():
    return {
        "model_package_json": {
            "name": "Demo regression model",
            "modelDescription": {"description": "Regression on demo dataset"},
            "target": {"type": TARGET_TYPE.REGRESSION, "name": 'Grade 2014'},
        }
    }


def test_operator_create_external_model_package_op(mocker, external_model_params):
    external_model_package_id = "test-external-model-package-id"

    create_external_model_package_mock = mocker.patch.object(
        CreateExternalModelPackageOperator,
        "create_model_package_from_json",
        return_value=external_model_package_id,
    )

    operator = CreateExternalModelPackageOperator(
        task_id='create_external_model_package',
    )

    operator_result = operator.execute(context={"params": external_model_params})

    create_external_model_package_mock.assert_called()

    assert operator_result == external_model_package_id


def test_operator_create_external_model_package_param_op(mocker, external_model_params):
    external_model_package_id = "test-external-model-package-id"

    create_external_model_package_mock = mocker.patch.object(
        CreateExternalModelPackageOperator,
        "create_model_package_from_json",
        return_value=external_model_package_id,
    )

    operator = CreateExternalModelPackageOperator(
        task_id='create_external_model_package', model_package_json=external_model_params
    )

    operator_result = operator.execute(context={"params": {}})

    create_external_model_package_mock.assert_called()

    assert operator_result == external_model_package_id


def test_operator_create_external_model_package_none_op(mocker):
    external_model_package_id = "test-external-model-package-id"

    create_external_model_package_mock = mocker.patch.object(
        CreateExternalModelPackageOperator,
        "create_model_package_from_json",
        return_value=external_model_package_id,
    )

    operator = CreateExternalModelPackageOperator(task_id='create_external_model_package')

    create_external_model_package_mock.assert_not_called()

    with pytest.raises(ValueError):
        operator.execute(context={"params": {}})
