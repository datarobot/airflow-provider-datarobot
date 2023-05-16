# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.connections import JDBCDataSourceHook


def test_datarobot_jdbc_get_conn(dr_jdbc_conn_details, mock_airflow_connection_datarobot_jdbc):
    hook = JDBCDataSourceHook(datarobot_jdbc_conn_id="datarobot_jdbc_default")
    dr_credentials, dr_datastore = hook.get_conn()

    assert dr_credentials["user"] == dr_jdbc_conn_details["login"]
    assert dr_credentials["password"] == dr_jdbc_conn_details["password"]
    assert dr_datastore.params.jdbc_url == dr_jdbc_conn_details["jdbc_url"]
