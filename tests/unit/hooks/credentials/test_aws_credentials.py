# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.credentials import AwsCredentialsHook


def test_datarobot_aws_credentials_conn(
    dr_aws_credentials_conn_details, mock_airflow_connection_datarobot_aws_credentials
):
    hook = AwsCredentialsHook(datarobot_credentials_conn_id="datarobot_aws_credentials_test")
    _, credential_data = hook.get_conn()

    assert credential_data["awsAccessKeyId"] == dr_aws_credentials_conn_details["aws_access_key_id"]
    assert (
        credential_data["awsSecretAccessKey"]
        == dr_aws_credentials_conn_details["aws_secret_access_key"]
    )
    assert (
        credential_data["awsSessionToken"] == dr_aws_credentials_conn_details["aws_session_token"]
    )
