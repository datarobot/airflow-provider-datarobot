# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.credentials import AzureStorageCredentialsHook


def test_datarobot_azure_credentials_conn(
    dr_azure_credentials_conn_details, mock_airflow_connection_datarobot_azure_credentials
):
    hook = AzureStorageCredentialsHook(
        datarobot_credentials_conn_id="datarobot_azure_credentials_test"
    )
    _, credential_data = hook.get_conn()

    assert (
        credential_data["azureConnectionString"]
        == dr_azure_credentials_conn_details["azure_connection_string"]
    )
