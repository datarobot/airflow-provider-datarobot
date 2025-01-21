# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import json
from typing import Any
from typing import Optional
from typing import Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from datarobot.models.credential import Credential
from datarobot.models.data_store import DataStore

from datarobot_provider.hooks.datarobot import DataRobotHook


class CredentialsBaseHook(BaseHook):
    """
    Base class for DataRobot credential hooks that interacts with DataRobot
    via its public Python API library to manage stored credentials.

    :param datarobot_credentials_conn_id: Connection ID
    :type datarobot_credentials_conn_id: str
    """

    conn_name_attr = "datarobot_credentials_conn_id"
    hook_name = "DataRobot Credentials"
    default_credential_description = "Credentials managed by Airflow provider for Datarobot"

    def __init__(
        self,
        datarobot_credentials_conn_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.datarobot_credentials_conn_id = datarobot_credentials_conn_id

    def create_credentials(self, conn) -> Credential:
        """Creates DataRobot Credentials."""
        raise NotImplementedError()

    def get_credential_data(self, conn) -> dict:
        """Get credential data dict to use with methods that
        accept credential_data instead of credential ID"""
        raise NotImplementedError()

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates DataRobot Credentials"""
        raise NotImplementedError()

    def get_or_create_credential(self, conn) -> Credential:
        # Trying to find existing DataRobot Credentials:
        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        for credential in Credential.list():
            if credential.name == self.datarobot_credentials_conn_id:
                self.log.info(
                    f"Found Existing Credentials :{credential.name} , id={credential.credential_id}"
                )
                if (
                    credential.description
                    and self.default_credential_description in credential.description
                ):
                    self.log.info(
                        f"Trying to update provided credential:{credential.name} using Airflow preconfigured"
                        f" credentials"
                    )
                    self.update_credentials(conn, credential)
                    break
                else:
                    raise AirflowException(
                        f"Found Existing Credentials :{credential.name} , id={credential.credential_id}"
                        f" not managed by Airflow provider: {credential.description}"
                    )
        else:
            self.log.info(
                f"Credentials:{self.datarobot_credentials_conn_id} does not exist, trying to create it"
            )
            credential = self.create_credentials(conn)
            self.log.info(
                f"Credentials:{self.datarobot_credentials_conn_id} successfully created, id={credential.credential_id}"
            )
        return credential

    def get_conn(self) -> Union[tuple[Credential, dict, DataStore], tuple[Credential, dict]]:
        """Get or Create DataRobot associated credentials managed by Airflow provider."""
        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        conn = self.get_connection(self.datarobot_credentials_conn_id)

        datarobot_connection_id = conn.extra_dejson.get("datarobot_connection", "")

        if not datarobot_connection_id:
            raise AirflowException("datarobot_connection is not defined")

        # Initialize DataRobot client by DataRobotHook
        DataRobotHook(datarobot_conn_id=datarobot_connection_id).run()

        credential = self.get_or_create_credential(conn)
        credential_data = self.get_credential_data(conn)

        return credential, credential_data

    def run(self) -> Any:
        # get Credentials with credential_data
        return self.get_conn()

    def test_connection(self):
        """Test that we can create DataRobot Credentials without errors"""
        try:
            credential, credential_data = self.run()
            credential = Credential.get(credential.credential_id)
            self.log.info(
                f"Test credential {credential.name} id={credential.credential_id} created"
            )
            # Airflow using randomly generated connection_id for to test connection,
            # so we need to delete it after creation:
            credential.delete()
            self.log.info(
                f"Test credential {credential.name} id={credential.credential_id} deleted"
            )
            return True, "Test creating DataRobot credentials success"
        except Exception as e:
            return False, str(e)


class BasicCredentialsHook(CredentialsBaseHook):
    hook_name = "DataRobot Basic Credentials"
    conn_type = "datarobot.credentials.basic"

    def __init__(
        self,
        datarobot_credentials_conn_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.datarobot_credentials_conn_id = datarobot_credentials_conn_id

    def create_credentials(self, conn) -> Credential:
        """Creates DataRobot Basic Credentials using provided login/password."""
        if not conn.login:
            raise AirflowException("login is not defined")

        if not conn.password:
            raise AirflowException("password is not defined")

        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        self.log.info(f"Creating Basic Credentials:{self.datarobot_credentials_conn_id}")
        credential = Credential.create_basic(
            name=self.datarobot_credentials_conn_id,
            user=conn.login,
            password=conn.password,
            description=self.default_credential_description,
        )

        return credential

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates DataRobot Basic Credentials using provided login/password."""
        basic_credentials = {
            "user": conn.login,
            "password": conn.password,
        }
        self.log.info(f"Updating Basic Credentials:{self.datarobot_credentials_conn_id}")

        try:
            credential.update(**basic_credentials)

        except Exception as e:
            self.log.error(
                f"Error updating Basic Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error updating Basic Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "basic",
            "user": conn.login,
            "password": conn.password,
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext("DataRobot Connection"),
                widget=BS3TextFieldWidget(),
                default="datarobot_default",
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "extra"],
            "relabeling": {},
            "placeholders": {
                "datarobot_connection": "datarobot_default",
                "login": "",
                "password": "",
            },
        }


class GoogleCloudCredentialsHook(CredentialsBaseHook):
    hook_name = "DataRobot GCP Credentials"
    conn_type = "datarobot.credentials.gcp"

    def parse_gcp_key(self, conn) -> dict:
        gcp_key = conn.extra_dejson.get("gcp_key", "")

        if not gcp_key:
            raise AirflowException("gcp_key is not defined")

        try:
            self.log.info("Trying to parse provided GCP key json")
            # removing newlines and parsing json:
            return json.loads(gcp_key.replace("\n", ""))

        except Exception as e:
            self.log.error(
                f"Error parsing GCP key json for credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error parsing GCP key json for credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def create_credentials(self, conn) -> Credential:
        """Returns Google Cloud credentials for params in connection object"""
        gcp_key_json = self.parse_gcp_key(conn)

        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        self.log.info(f"Creating Google Cloud Credentials:{self.datarobot_credentials_conn_id}")
        credential = Credential.create_gcp(
            name=self.datarobot_credentials_conn_id,
            gcp_key=gcp_key_json,
            description=self.default_credential_description,
        )
        return credential

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates a Google Cloud credentials for params in connection object"""

        gcp_credentials = {
            "gcp_key": self.parse_gcp_key(conn),
        }

        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        self.log.info(f"Updating Google Cloud Credentials:{self.datarobot_credentials_conn_id}")
        try:
            credential.update(**gcp_credentials)  # type: ignore

        except Exception as e:
            self.log.error(
                f"Error updating Google Cloud Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error updating Google Cloud Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "gcp",
            "gcpKey": conn.extra_dejson.get("gcp_key", ""),
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext("DataRobot Connection"),
                widget=BS3TextFieldWidget(),
                default="datarobot_default",
            ),
            "gcp_key": StringField(
                lazy_gettext("GCP Key json content (Service Account)"),
                widget=BS3PasswordFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "login", "password", "extra"],
            "relabeling": {},
            "placeholders": {
                "datarobot_connection": "datarobot_default",
                "gcp_key": "Enter a valid JSON string",
            },
        }


class AwsCredentialsHook(CredentialsBaseHook):
    hook_name = "DataRobot AWS Credentials"
    conn_type = "datarobot.credentials.aws"

    def create_credentials(self, conn) -> Credential:
        """Returns AWS credentials for params in connection object"""

        if not conn.login:
            raise AirflowException("aws_access_key_id is not defined")

        if not conn.password:
            raise AirflowException("aws_secret_access_key is not defined")

        # aws_session_token is optional:
        aws_session_token = conn.extra_dejson.get("aws_session_token", None)

        try:
            if not self.datarobot_credentials_conn_id:
                raise AirflowException("datarobot_credentials_conn_id is not defined")

            self.log.info(f"Creating AWS Credentials:{self.datarobot_credentials_conn_id}")
            credential = Credential.create_s3(
                name=self.datarobot_credentials_conn_id,
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                aws_session_token=aws_session_token,
                description=self.default_credential_description,
            )

            return credential

        except Exception as e:
            self.log.error(
                f"Error creating AWS Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error creating AWS Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates AWS credentials for params in connection object"""

        if not conn.login:
            raise AirflowException("aws_access_key_id is not defined")

        if not conn.password:
            raise AirflowException("aws_secret_access_key is not defined")

        # aws_session_token is optional:
        aws_session_token = conn.extra_dejson.get("aws_session_token", None)

        try:
            self.log.info(f"Updating AWS Credentials:{self.datarobot_credentials_conn_id}")

            aws_credentials = {
                "aws_access_key_id": conn.login,
                "aws_secret_access_key": conn.password,
                "aws_session_token": aws_session_token,
            }

            credential.update(**aws_credentials)

        except Exception as e:
            self.log.error(
                f"Error updating AWS Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error updating AWS Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "s3",
            "awsAccessKeyId": conn.login,
            "awsSecretAccessKey": conn.password,
        }
        aws_session_token = conn.extra_dejson.get("aws_session_token", "")

        if aws_session_token:
            # if AWS Session Token is not empty:
            credential_data["awsSessionToken"] = aws_session_token
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext("DataRobot Connection"),
                widget=BS3TextFieldWidget(),
                default="datarobot_default",
            ),
            "aws_session_token": StringField(
                lazy_gettext("AWS session token"),
                widget=BS3TextAreaFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "extra"],
            "relabeling": {
                "login": "AWS Access Key ID",
                "password": "AWS Secret Access Key",
            },
            "placeholders": {"datarobot_connection": "datarobot_default"},
        }


class AzureStorageCredentialsHook(CredentialsBaseHook):
    hook_name = "DataRobot Azure Storage Credentials"
    conn_type = "datarobot.credentials.azure"

    def create_azure_connection_string(self, conn) -> str:
        if not conn.login:
            raise AirflowException("Storage Account Name is not defined")

        if not conn.password:
            raise AirflowException("Storage Account Key is not defined")

        azure_connection_string = "DefaultEndpointsProtocol=https;"
        "AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(
            conn.login, conn.password
        )
        return azure_connection_string

    def create_credentials(self, conn) -> Credential:
        """Returns Azure Storage credentials for params in connection object"""

        try:
            if not self.datarobot_credentials_conn_id:
                raise AirflowException("datarobot_credentials_conn_id is not defined")

            self.log.info(
                f"Creating Azure Storage Credentials: {self.datarobot_credentials_conn_id}"
            )
            credential = Credential.create_azure(
                name=self.datarobot_credentials_conn_id,
                azure_connection_string=self.create_azure_connection_string(conn),
                description=self.default_credential_description,
            )

            return credential

        except Exception as e:
            self.log.error(
                f"Error creating Azure Credentials: {self.datarobot_credentials_conn_id}, message:{e}"
            )
            raise AirflowException(
                f"Error creating Azure Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates Azure Storage credentials for params in connection object"""

        azure_credentials = {"azure_connection_string": self.create_azure_connection_string(conn)}
        self.log.info(f"Updating Azure Storage Credentials: {self.datarobot_credentials_conn_id}")
        try:
            credential.update(**azure_credentials)

        except Exception as e:
            self.log.error(
                f"Error updating Azure Storage Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error updating Azure Storage Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "azure",
            "azureConnectionString": "DefaultEndpointsProtocol=https;"
            "AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(
                conn.login, conn.password
            ),
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext("DataRobot Connection"),
                widget=BS3TextFieldWidget(),
                default="datarobot_default",
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "extra"],
            "relabeling": {
                "login": "Azure Storage Account Name",
                "password": "Azure Storage Account Key",
            },
            "placeholders": {"datarobot_connection": "datarobot_default"},
        }


class OAuthCredentialsHook(CredentialsBaseHook):
    hook_name = "DataRobot OAuth Credentials"
    conn_type = "datarobot.credentials.oauth"

    def create_credentials(self, conn) -> Credential:
        """Returns OAuth credentials for params in connection object"""

        if not conn.login:
            raise AirflowException("OAuth Client Id is not defined")

        if not conn.password:
            raise AirflowException("OAuth Token is not defined")

        refresh_token = conn.extra_dejson.get("refresh_token", "")
        if not refresh_token:
            raise AirflowException("OAuth Refresh Token is not defined")

        try:
            if not self.datarobot_credentials_conn_id:
                raise AirflowException("datarobot_credentials_conn_id is not defined")

            self.log.info(f"Creating OAuth Credentials: {self.datarobot_credentials_conn_id}")
            credential = Credential.create_oauth(
                name=self.datarobot_credentials_conn_id,
                token=conn.password,
                refresh_token=refresh_token,
                description=self.default_credential_description,
            )

            return credential

        except Exception as e:
            self.log.error(
                f"Error creating OAuth Credentials: {self.datarobot_credentials_conn_id}, message:{e}"
            )
            raise AirflowException(
                f"Error creating OAuth Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def update_credentials(self, conn, credential: Credential) -> None:
        """Updates OAuth credentials for params in connection object"""

        if not conn.password:
            raise AirflowException("OAuth Token is not defined")

        refresh_token = conn.extra_dejson.get("refresh_token", "")
        if not refresh_token:
            raise AirflowException("OAuth Refresh Token is not defined")

        oauth_credentials = {
            "token": conn.password,
            "refresh_token": refresh_token,
        }

        self.log.info(f"Updating OAuth Credentials: {self.datarobot_credentials_conn_id}")
        try:
            credential.update(**oauth_credentials)

        except Exception as e:
            self.log.error(
                f"Error updating OAuth Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error updating OAuth Credentials: {self.datarobot_credentials_conn_id}"
            ) from None

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "oauth",
            "oauthRefreshToken": conn.extra_dejson.get("refresh_token", ""),
            "oauthClientId": conn.login,
            "oauthClientSecret": conn.password,
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "refresh_token": StringField(
                lazy_gettext("OAuth Refresh Token"),
                widget=BS3PasswordFieldWidget(),
            ),
            "datarobot_connection": StringField(
                lazy_gettext("DataRobot Connection"),
                widget=BS3TextFieldWidget(),
                default="datarobot_default",
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "port", "extra"],
            "relabeling": {
                "login": "OAuth Client Id",
                "password": "OAuth Token",
            },
            "placeholders": {"datarobot_connection": "datarobot_default"},
        }
