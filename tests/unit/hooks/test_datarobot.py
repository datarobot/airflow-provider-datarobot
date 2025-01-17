# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datarobot_provider.hooks.datarobot import DataRobotHook


def test_datarobot_get_conn(dr_conn_details):
    hook = DataRobotHook(datarobot_conn_id='datarobot_default')
    dr_client = hook.get_conn()

    assert dr_client.endpoint == dr_conn_details['endpoint']
    assert dr_client.token == dr_conn_details['api_key']
