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


# For JDBC Connection
@pytest.fixture
def dr_jdbc_conn_details():
    return {
        "login": "test_login",
        "password": "test_password",
        "datarobot_connection": "datarobot_default",
        "jdbc_driver": "test JDBC Driver",
        "jdbc_url": "jdbc:test_jdbc_connection_string",
    }


# For Basic Credentials test
@pytest.fixture
def dr_basic_credentials_conn_details():
    return {
        "login": "test_login",
        "password": "test_password",
        "datarobot_connection": "datarobot_default",
    }


# For GCP Credentials test
@pytest.fixture
def dr_gcp_credentials_conn_details():
    return {
        "gcp_key": '{"gcp_credentials":"test"}',
        "datarobot_connection": "datarobot_default",
    }


@pytest.fixture(autouse=True)
def mock_datarobot_driver(mocker, dr_jdbc_conn_details):
    driver_list_mock = [
        mocker.Mock(
            id='test-jdbc-driver-id',
            canonical_name='test JDBC Driver',
        )
    ]

    mocker.patch(
        "datarobot_provider.hooks.connections.DataDriver.list", return_value=driver_list_mock
    )


@pytest.fixture(scope="function", autouse=True)
def mock_datarobot_datastore(mocker, dr_jdbc_conn_details):
    datastore_create_mock = mocker.Mock(
        id='test-datastore-id',
        canonical_name='datarobot_jdbc_default',
        params=mocker.Mock(
            driver_id='test-jdbc-driver-id', jdbc_url=dr_jdbc_conn_details["jdbc_url"]
        ),
    )

    mocker.patch("datarobot_provider.hooks.connections.DataStore.list", return_value=[])
    mocker.patch(
        "datarobot_provider.hooks.connections.DataStore.create", return_value=datastore_create_mock
    )


@pytest.fixture(scope="function", autouse=True)
def mock_airflow_connection_datarobot_jdbc(mocker, dr_jdbc_conn_details):
    conn = Connection(
        conn_type="datarobot_jdbc_datasource",
        login=dr_jdbc_conn_details["login"],
        password=dr_jdbc_conn_details["password"],
        extra=json.dumps(
            {
                "datarobot_connection": dr_jdbc_conn_details["datarobot_connection"],
                "jdbc_driver": dr_jdbc_conn_details["jdbc_driver"],
                "jdbc_url": dr_jdbc_conn_details["jdbc_url"],
            }
        ),
    )
    mocker.patch.dict("os.environ", AIRFLOW_CONN_DATAROBOT_JDBC_DEFAULT=conn.get_uri())


@pytest.fixture(scope="function", autouse=True)
def mock_datarobot_basic_credentials(mocker, dr_basic_credentials_conn_details):
    credentials_create_mock = mocker.Mock(
        credential_id='test-credentials-id',
        name='datarobot_basic_credentials_default',
        credential_type='basic',
        user=dr_basic_credentials_conn_details["login"],
        password=dr_basic_credentials_conn_details["password"],
        description="Credentials managed by Airflow provider for Datarobot",
    )

    mocker.patch("datarobot_provider.hooks.credentials.Credential.list", return_value=[])
    mocker.patch(
        "datarobot_provider.hooks.credentials.Credential.create_basic",
        return_value=credentials_create_mock,
    )


@pytest.fixture(scope="function", autouse=True)
def mock_airflow_connection_datarobot_basic_credentials(mocker, dr_basic_credentials_conn_details):
    conn = Connection(
        conn_type="datarobot_basic_credentials",
        login=dr_basic_credentials_conn_details["login"],
        password=dr_basic_credentials_conn_details["password"],
        extra=json.dumps(
            {
                "datarobot_connection": dr_basic_credentials_conn_details["datarobot_connection"],
            }
        ),
    )
    mocker.patch.dict("os.environ", AIRFLOW_CONN_DATAROBOT_BASIC_CREDENTIALS_DEFAULT=conn.get_uri())


@pytest.fixture(scope="function", autouse=True)
def mock_datarobot_gcp_credentials(mocker, dr_gcp_credentials_conn_details):
    gcp_credentials_create_mock = mocker.Mock(
        credential_id='test-gcp-credentials-id',
        name='datarobot_gcp_credentials_test',
        credential_type='gcp',
        gcp_key=dr_gcp_credentials_conn_details["gcp_key"],
        description="Credentials managed by Airflow provider for Datarobot",
    )

    mocker.patch("datarobot_provider.hooks.credentials.Credential.list", return_value=[])
    mocker.patch(
        "datarobot_provider.hooks.credentials.Credential.create_gcp",
        return_value=gcp_credentials_create_mock,
    )


@pytest.fixture(scope="function", autouse=True)
def mock_airflow_connection_datarobot_gcp_credentials(mocker, dr_gcp_credentials_conn_details):
    conn = Connection(
        conn_type="datarobot_gcp_credentials",
        extra=json.dumps(
            {
                "gcp_key": dr_gcp_credentials_conn_details["gcp_key"],
                "datarobot_connection": dr_gcp_credentials_conn_details["datarobot_connection"],
            }
        ),
    )
    mocker.patch.dict("os.environ", AIRFLOW_CONN_DATAROBOT_GCP_CREDENTIALS_TEST=conn.get_uri())
