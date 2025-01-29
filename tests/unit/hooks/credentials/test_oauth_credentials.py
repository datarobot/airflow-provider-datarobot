# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.credentials import OAuthCredentialsHook


def test_datarobot_oauth_credentials_conn(
    dr_oauth_credentials_conn_details, mock_airflow_connection_datarobot_oauth_credentials
):
    hook = OAuthCredentialsHook(datarobot_credentials_conn_id="datarobot_oauth_credentials_test")
    _, credential_data = hook.get_conn()

    assert credential_data["oauthClientId"] == dr_oauth_credentials_conn_details["oauth_client_id"]
    assert credential_data["oauthClientSecret"] == dr_oauth_credentials_conn_details["token"]
    assert (
        credential_data["oauthRefreshToken"] == dr_oauth_credentials_conn_details["refresh_token"]
    )
