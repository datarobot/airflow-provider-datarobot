# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.credentials import GoogleCloudCredentialsHook


def test_datarobot_gcp_credentials_conn(
    dr_gcp_credentials_conn_details, mock_airflow_connection_datarobot_gcp_credentials
):
    hook = GoogleCloudCredentialsHook(
        datarobot_credentials_conn_id="datarobot_gcp_credentials_test"
    )
    _, credential_data = hook.get_conn()

    assert credential_data["gcpKey"] == dr_gcp_credentials_conn_details["gcp_key"]
