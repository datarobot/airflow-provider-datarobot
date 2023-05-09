# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.credentials import GetCredentialIdOperator


def test_operator_get_credential_id(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "credential-id"
    credential_mock.name = "datarobot_basic_credentials_default"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])

    operator = GetCredentialIdOperator(task_id='get_credentials')
    credential_id = operator.execute(
        context={
            "params": {
                "datarobot_credentials_name": "datarobot_basic_credentials_default",
            },
        }
    )

    assert credential_id == "credential-id"


def test_operator_get_credential_not_found(mocker):
    credential_mock = mocker.Mock()
    credential_mock.credential_id = "credential-id"
    credential_mock.name = "datarobot_basic_credentials_not_found"
    mocker.patch.object(dr.Credential, "list", return_value=[credential_mock])
    # should raise ValueError if credentials with provided name is not found
    with pytest.raises(ValueError):
        operator = GetCredentialIdOperator(task_id='get_credentials')
        operator.execute(
            context={
                "params": {
                    "datarobot_credentials_name": "datarobot_basic_credentials_default",
                },
            }
        )
