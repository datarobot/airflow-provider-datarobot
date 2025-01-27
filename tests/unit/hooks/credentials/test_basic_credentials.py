# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.credentials import BasicCredentialsHook


def test_datarobot_basic_credentials_conn(
    dr_basic_credentials_conn_details, mock_airflow_connection_datarobot_basic_credentials
):
    hook = BasicCredentialsHook(datarobot_credentials_conn_id="datarobot_basic_credentials_test")
    _, credential_data = hook.get_conn()

    assert credential_data["user"] == dr_basic_credentials_conn_details["login"]
    assert credential_data["password"] == dr_basic_credentials_conn_details["password"]
