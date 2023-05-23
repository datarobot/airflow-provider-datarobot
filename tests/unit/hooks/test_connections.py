# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.connections import JDBCDataSourceHook


def test_datarobot_jdbc_get_conn(
    dr_jdbc_conn_details,
    dr_basic_credentials_conn_details,
    mock_airflow_connection_datarobot_jdbc,
    mock_datarobot_basic_credentials,
    mock_datarobot_datastore,
):
    hook = JDBCDataSourceHook(datarobot_credentials_conn_id="datarobot_test_connection_jdbc_test")
    dr_credentials, dr_credentials_data, dr_datastore = hook.get_conn()

    assert dr_credentials.credential_id == 'test-credentials-id'
    assert dr_credentials_data["user"] == dr_jdbc_conn_details["login"]
    assert dr_credentials_data["password"] == dr_jdbc_conn_details["password"]
    assert dr_datastore.params.jdbc_url == dr_jdbc_conn_details["jdbc_url"]
