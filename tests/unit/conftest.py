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
    return {'endpoint': 'https://app.datarobot.com/api/v2', 'api_key': 'my-api-key'}


@pytest.fixture(autouse=True)
def mock_datarobot_client(mocker, dr_conn_details):
    client_mock = mocker.Mock(
        endpoint=dr_conn_details['endpoint'], token=dr_conn_details['api_key']
    )
    mocker.patch('datarobot_provider.hooks.datarobot.Client', return_value=client_mock)


@pytest.fixture(autouse=True)
def mock_airflow_connection(mocker, dr_conn_details):
    conn = Connection(
        conn_type='DataRobot',
        extra=json.dumps(
            {
                'extra__http__endpoint': dr_conn_details['endpoint'],
                'extra__http__api_key': dr_conn_details['api_key'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_DEFAULT=conn.get_uri())


# For Basic Credentials test
@pytest.fixture
def dr_basic_credentials_conn_details():
    return {
        'login': 'test_login',
        'password': 'test_password',
        'datarobot_connection': 'datarobot_default',
    }


@pytest.fixture()
def mock_datarobot_basic_credentials(mocker):
    credentials_create_mock = mocker.Mock(
        credential_id='test-credentials-id',
        name='datarobot_basic_credentials_test',
        credential_type='basic',
        description='Credentials managed by Airflow provider for Datarobot',
    )

    mocker.patch('datarobot_provider.hooks.credentials.Credential.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.credentials.Credential.create_basic',
        return_value=credentials_create_mock,
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_basic_credentials(
    mocker, dr_basic_credentials_conn_details, mock_datarobot_basic_credentials
):
    conn = Connection(
        conn_type='datarobot.credentials.basic',
        login=dr_basic_credentials_conn_details['login'],
        password=dr_basic_credentials_conn_details['password'],
        extra=json.dumps(
            {
                'datarobot_connection': dr_basic_credentials_conn_details['datarobot_connection'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_BASIC_CREDENTIALS_TEST=conn.get_uri())


# For GCP Credentials test
@pytest.fixture
def dr_gcp_credentials_conn_details():
    return {
        'gcp_key': '{"gcp_credentials":"test"}',
        'datarobot_connection': 'datarobot_default',
    }


@pytest.fixture()
def mock_datarobot_gcp_credentials(mocker):
    gcp_credentials_create_mock = mocker.Mock(
        credential_id='test-gcp-credentials-id',
        name='datarobot_gcp_credentials_test',
        credential_type='gcp',
        description='Credentials managed by Airflow provider for Datarobot',
    )

    mocker.patch('datarobot_provider.hooks.credentials.Credential.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.credentials.Credential.create_gcp',
        return_value=gcp_credentials_create_mock,
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_gcp_credentials(
    mocker, dr_gcp_credentials_conn_details, mock_datarobot_gcp_credentials
):
    conn = Connection(
        conn_type='datarobot.credentials.gcp',
        extra=json.dumps(
            {
                'gcp_key': dr_gcp_credentials_conn_details['gcp_key'],
                'datarobot_connection': dr_gcp_credentials_conn_details['datarobot_connection'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_GCP_CREDENTIALS_TEST=conn.get_uri())


# For AWS Credentials test
@pytest.fixture
def dr_aws_credentials_conn_details():
    return {
        'aws_access_key_id': 'test_aws_access_key_id',
        'aws_secret_access_key': 'test_aws_secret_access_key',
        'aws_session_token': 'test_aws_session_token',
        'datarobot_connection': 'datarobot_default',
    }


@pytest.fixture()
def mock_datarobot_aws_credentials(mocker):
    aws_credentials_create_mock = mocker.Mock(
        credential_id='test-aws-credentials-id',
        name='datarobot_aws_credentials_test',
        credential_type='s3',
        description='Credentials managed by Airflow provider for Datarobot',
    )

    mocker.patch('datarobot_provider.hooks.credentials.Credential.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.credentials.Credential.create_s3',
        return_value=aws_credentials_create_mock,
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_aws_credentials(
    mocker, dr_aws_credentials_conn_details, mock_datarobot_aws_credentials
):
    conn = Connection(
        conn_type='datarobot.credentials.aws',
        login=dr_aws_credentials_conn_details['aws_access_key_id'],
        password=dr_aws_credentials_conn_details['aws_secret_access_key'],
        extra=json.dumps(
            {
                'aws_session_token': dr_aws_credentials_conn_details['aws_session_token'],
                'datarobot_connection': dr_aws_credentials_conn_details['datarobot_connection'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_AWS_CREDENTIALS_TEST=conn.get_uri())


# For Azure Storage Credentials test
@pytest.fixture
def dr_azure_credentials_conn_details():
    return {
        'azure_storage_account_name': 'test_azure_account_name',
        'azure_storage_account_key': 'test_azure_account_key',
        'datarobot_connection': 'datarobot_default',
        'azure_connection_string': 'DefaultEndpointsProtocol=https;AccountName=test_azure_account_name;AccountKey'
        '=test_azure_account_key;EndpointSuffix=core.windows.net',
    }


@pytest.fixture()
def mock_datarobot_azure_credentials(mocker):
    azure_credentials_create_mock = mocker.Mock(
        credential_id='test-azure-credentials-id',
        name='datarobot_azure_credentials_test',
        credential_type='azure',
        description='Credentials managed by Airflow provider for Datarobot',
    )

    mocker.patch('datarobot_provider.hooks.credentials.Credential.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.credentials.Credential.create_azure',
        return_value=azure_credentials_create_mock,
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_azure_credentials(
    mocker, dr_azure_credentials_conn_details, mock_datarobot_azure_credentials
):
    conn = Connection(
        conn_type='datarobot.credentials.azure',
        login=dr_azure_credentials_conn_details['azure_storage_account_name'],
        password=dr_azure_credentials_conn_details['azure_storage_account_key'],
        extra=json.dumps(
            {
                'datarobot_connection': dr_azure_credentials_conn_details['datarobot_connection'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_AZURE_CREDENTIALS_TEST=conn.get_uri())


# For OAuth Credentials test
@pytest.fixture
def dr_oauth_credentials_conn_details():
    return {
        'oauth_client_id': 'test_oauth_client_id',
        'token': 'test_oauth_token',
        'refresh_token': 'test_oauth_refresh_token',
        'datarobot_connection': 'datarobot_default',
    }


@pytest.fixture()
def mock_datarobot_oauth_credentials(mocker):
    oauth_credentials_create_mock = mocker.Mock(
        credential_id='test-oauth-credentials-id',
        name='datarobot_oauth_credentials_test',
        credential_type='oauth',
        description='Credentials managed by Airflow provider for Datarobot',
    )

    mocker.patch('datarobot_provider.hooks.credentials.Credential.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.credentials.Credential.create_oauth',
        return_value=oauth_credentials_create_mock,
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_oauth_credentials(
    mocker, dr_oauth_credentials_conn_details, mock_datarobot_oauth_credentials
):
    conn = Connection(
        conn_type='datarobot.credentials.oauth',
        login=dr_oauth_credentials_conn_details['oauth_client_id'],
        password=dr_oauth_credentials_conn_details['token'],
        extra=json.dumps(
            {
                'datarobot_connection': dr_oauth_credentials_conn_details['datarobot_connection'],
                'refresh_token': dr_oauth_credentials_conn_details['refresh_token'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_OAUTH_CREDENTIALS_TEST=conn.get_uri())


# For JDBC Connection
@pytest.fixture
def dr_jdbc_conn_details():
    return {
        'login': 'test_login',
        'password': 'test_password',
        'datarobot_connection': 'datarobot_default',
        'jdbc_driver': 'test JDBC Driver',
        'jdbc_url': 'jdbc:test_jdbc_connection_string',
    }


@pytest.fixture()
def mock_datarobot_driver(mocker):
    driver_list_mock = [
        mocker.Mock(
            id='test-jdbc-driver-id',
            canonical_name='test JDBC Driver',
        )
    ]

    mocker.patch(
        'datarobot_provider.hooks.connections.DataDriver.list', return_value=driver_list_mock
    )


@pytest.fixture()
def mock_datarobot_datastore(mocker, dr_jdbc_conn_details, mock_datarobot_driver):
    datastore_create_mock = mocker.Mock(
        id='test-datastore-id',
        canonical_name='datarobot_test_connection_jdbc_test',
        params=mocker.Mock(
            driver_id='test-jdbc-driver-id', jdbc_url=dr_jdbc_conn_details['jdbc_url']
        ),
    )

    mocker.patch('datarobot_provider.hooks.connections.DataStore.list', return_value=[])
    mocker.patch(
        'datarobot_provider.hooks.connections.DataStore.create', return_value=datastore_create_mock
    )


@pytest.fixture()
def mock_airflow_connection_datarobot_jdbc(
    mocker, dr_jdbc_conn_details, mock_datarobot_basic_credentials, mock_datarobot_datastore
):
    conn = Connection(
        conn_type='datarobot.datasource.jdbc',
        login=dr_jdbc_conn_details['login'],
        password=dr_jdbc_conn_details['password'],
        extra=json.dumps(
            {
                'datarobot_connection': dr_jdbc_conn_details['datarobot_connection'],
                'jdbc_driver': dr_jdbc_conn_details['jdbc_driver'],
                'jdbc_url': dr_jdbc_conn_details['jdbc_url'],
            }
        ),
    )
    mocker.patch.dict('os.environ', AIRFLOW_CONN_DATAROBOT_TEST_CONNECTION_JDBC_TEST=conn.get_uri())
