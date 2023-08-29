# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.execution_environment import CreateExecutionEnvironmentOperator


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
