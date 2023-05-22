# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from airflow.exceptions import AirflowNotFoundException

from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator


def test_operator_get_credential_not_found(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "credential-id"
    credential_mock.name = "datarobot_basic_credentials_test"
    credential_mock.credential_type = 'datarobot.credentials.basic'
    credential_mock.description = "Credentials managed by Airflow provider for Datarobot"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])
    # should raise ValueError if credentials with provided name is not found
    with pytest.raises(AirflowNotFoundException):
        operator = GetOrCreateCredentialOperator(task_id='get_credentials')
        operator.execute(
            context={
                "params": {
                    "datarobot_credentials_name": "datarobot_basic_credentials_not_exist",
                },
            }
        )


def test_operator_get_basic_credential_id(
    mocker, mock_airflow_connection_datarobot_basic_credentials
):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "credential-id"
    credential_mock.credential_type = 'datarobot.credentials.basic'
    credential_mock.name = "datarobot_basic_credentials_test"
    credential_mock.description = "Credentials managed by Airflow provider for Datarobot"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])

    operator = GetOrCreateCredentialOperator(task_id='get_credentials')
    credential_id = operator.execute(
        context={
            "params": {
                "datarobot_credentials_name": "datarobot_basic_credentials_test",
            },
        }
    )

    assert credential_id == "credential-id"


def test_operator_get_gcp_credential_id(mocker, mock_airflow_connection_datarobot_gcp_credentials):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "test-gcp-credentials-id"
    credential_mock.credential_type = 'datarobot.credentials.gcp'
    credential_mock.name = "datarobot_gcp_credentials_test"
    credential_mock.description = "Credentials managed by Airflow provider for Datarobot"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])

    operator = GetOrCreateCredentialOperator(task_id='get_credentials')
    credential_id = operator.execute(
        context={
            "params": {
                "datarobot_credentials_name": "datarobot_gcp_credentials_test",
            },
        }
    )

    assert credential_id == "test-gcp-credentials-id"


def test_operator_get_gcp_credential_not_found(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_type = 'datarobot.credentials.gcp'
    credential_mock.credential_id = "test-gcp-credentials-id"
    credential_mock.name = "datarobot_gcp_credentials_test"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])
    # should raise ValueError if credentials with provided name is not found
    with pytest.raises(AirflowNotFoundException):
        operator = GetOrCreateCredentialOperator(task_id='get_credentials')
        operator.execute(
            context={
                "params": {
                    "datarobot_credentials_name": "datarobot_gcp_credentials_not_exist",
                },
            }
        )


def test_operator_get_aws_credential_id(mocker, mock_airflow_connection_datarobot_aws_credentials):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "test-aws-credentials-id"
    credential_mock.credential_type = 'datarobot.credentials.aws'
    credential_mock.name = "datarobot_aws_credentials_test"
    credential_mock.description = "Credentials managed by Airflow provider for Datarobot"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])

    operator = GetOrCreateCredentialOperator(task_id='get_credentials')
    credential_id = operator.execute(
        context={
            "params": {
                "datarobot_credentials_name": "datarobot_aws_credentials_test",
            },
        }
    )

    assert credential_id == "test-aws-credentials-id"


def test_operator_get_aws_credential_not_found(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_type = 'datarobot.credentials.aws'
    credential_mock.credential_id = "test-aws-credentials-id"
    credential_mock.name = "datarobot_aws_credentials_test"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])
    # should raise ValueError if credentials with provided name is not found
    with pytest.raises(AirflowNotFoundException):
        operator = GetOrCreateCredentialOperator(task_id='get_credentials')
        operator.execute(
            context={
                "params": {
                    "datarobot_credentials_name": "datarobot_aws_credentials_not_exist",
                },
            }
        )


def test_operator_get_azure_credential_id(
    mocker, mock_airflow_connection_datarobot_azure_credentials
):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "test-azure-credentials-id"
    credential_mock.credential_type = 'datarobot.credentials.azure'
    credential_mock.name = "datarobot_azure_credentials_test"
    credential_mock.description = "Credentials managed by Airflow provider for Datarobot"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])

    operator = GetOrCreateCredentialOperator(task_id='get_credentials')
    credential_id = operator.execute(
        context={
            "params": {
                "datarobot_credentials_name": "datarobot_azure_credentials_test",
            },
        }
    )

    assert credential_id == "test-azure-credentials-id"


def test_operator_get_azure_credential_not_found(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_type = 'datarobot.credentials.azure'
    credential_mock.credential_id = "test-azure-credentials-id"
    credential_mock.name = "datarobot_azure_credentials_test"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])
    # should raise ValueError if credentials with provided name is not found
    with pytest.raises(AirflowNotFoundException):
        operator = GetOrCreateCredentialOperator(task_id='get_credentials')
        operator.execute(
            context={
                "params": {
                    "datarobot_credentials_name": "datarobot_azure_credentials_not_exist",
                },
            }
        )
