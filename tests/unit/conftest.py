# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import json

import pytest
from airflow.models import Connection


@pytest.fixture
def dr_conn_details():
    return {"endpoint": "https://app.datarobot.com/api/v2", "api_key": "my-api-key"}


@pytest.fixture(scope="function", autouse=True)
def mock_datarobot_client(mocker, dr_conn_details):
    client_mock = mocker.Mock(
        endpoint=dr_conn_details["endpoint"], token=dr_conn_details["api_key"]
    )
    mocker.patch("datarobot_provider.hooks.datarobot.Client", return_value=client_mock)


@pytest.fixture(scope="function", autouse=True)
def mock_airflow_connection(mocker, dr_conn_details):
    conn = Connection(
        conn_type="DataRobot",
        extra=json.dumps(
            {
                "extra__http__endpoint": dr_conn_details["endpoint"],
                "extra__http__api_key": dr_conn_details["api_key"],
            }
        ),
    )
    mocker.patch.dict("os.environ", AIRFLOW_CONN_DATAROBOT_DEFAULT=conn.get_uri())
