# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.custom_models import CreateExecutionEnvironmentOperator
from datarobot_provider.operators.custom_models import CreateExecutionEnvironmentVersionOperator


@pytest.fixture
def execution_environment_params():
    return {
        "execution_environment_name": "Demo Execution Environment",
        "execution_environment_description": "Demo Execution Environment for Airflow provider",
        "programming_language": "python",
    }


def test_operator_create_execution_environment(mocker, execution_environment_params):
    execution_environment_mock = mocker.Mock(target=None)
    execution_environment_mock.id = "test-execution-environment-id"

    execution_environment_create_mock = mocker.patch.object(
        dr.ExecutionEnvironment, "create", return_value=execution_environment_mock
    )

    operator = CreateExecutionEnvironmentOperator(
        task_id='create_execution_environment',
    )

    operator_result = operator.execute(context={"params": execution_environment_params})

    execution_environment_create_mock.assert_called_with(
        name=execution_environment_params["execution_environment_name"],
        description=execution_environment_params["execution_environment_description"],
        programming_language=execution_environment_params["programming_language"],
        required_metadata_keys=[],
    )

    assert operator_result == execution_environment_mock.id


@pytest.fixture
def execution_environment_version_params():
    return {
        "docker_context_path": "./datarobot-user-models-master/",
        "environment_version_description": "created by Airflow provider",
        "environment_version_label": "demo",
    }


def test_operator_create_execution_environment_version(
    mocker, execution_environment_version_params
):
    execution_environment_version_id = "test-execution-environment-version-id"
    max_wait_sec = 1800
    execution_environment_version_mock = mocker.Mock(target=None)
    execution_environment_version_mock.id = execution_environment_version_id

    execution_environment_version_create_mock = mocker.patch.object(
        dr.ExecutionEnvironmentVersion, "create", return_value=execution_environment_version_mock
    )

    operator = CreateExecutionEnvironmentVersionOperator(
        task_id='create_execution_environment_version',
        execution_environment_id=execution_environment_version_id,
        max_wait_sec=max_wait_sec,
    )

    operator_result = operator.execute(context={"params": execution_environment_version_params})

    execution_environment_version_create_mock.assert_called_with(
        execution_environment_id=execution_environment_version_id,
        docker_context_path=execution_environment_version_params["docker_context_path"],
        label=execution_environment_version_params["environment_version_label"],
        description=execution_environment_version_params["environment_version_description"],
        max_wait=max_wait_sec,
    )

    assert operator_result == execution_environment_version_mock.id
