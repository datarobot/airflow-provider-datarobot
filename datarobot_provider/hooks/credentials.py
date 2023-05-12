# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import json
from typing import Any
from typing import Dict

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from datarobot.models.credential import Credential

from datarobot_provider.hooks.datarobot import DataRobotHook


class CredentialsBaseHook(BaseHook):
    """
    Base class for DataRobot credential hooks that interacts with DataRobot
    via its public Python API library to manage stored credentials.

    :param datarobot_credentials_conn_id: Connection ID, defaults to `datarobot_credentials_default`
    :type datarobot_credentials_conn_id: str, optional
    """

    conn_name_attr = 'datarobot_credentials_conn_id'
    hook_name = 'DataRobot Credentials'
    default_credential_description = "Credentials managed by Airflow provider for Datarobot"

    def __init__(
        self,
        datarobot_credentials_conn_id: str = None,
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

    def get_conn(self) -> Credential:
        """Get or Create DataRobot associated credentials managed by Airflow provider."""

        conn = self.get_connection(self.datarobot_credentials_conn_id)

        datarobot_connection_id = conn.extra_dejson.get('datarobot_connection', '')

        if not datarobot_connection_id:
            raise AirflowException("datarobot_connection is not defined")

        # Initialize DataRobot client by DataRobotHook
        DataRobotHook(datarobot_conn_id=datarobot_connection_id).run()

        # Trying to find existing DataRobot Credentials managed by Airflow provider:
        for credential in Credential.list():
            if credential.name == self.datarobot_credentials_conn_id:
                if self.default_credential_description in credential.description:
                    self.log.info(
                        f"Found Existing Credentials :{credential.name} , id={credential.credential_id}"
                    )
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
    hook_name = 'DataRobot Basic Credentials'
    conn_type = 'datarobot.credentials.basic'

    def __init__(
        self,
        datarobot_credentials_conn_id: str = None,
    ) -> None:
        super().__init__()
        self.datarobot_credentials_conn_id = datarobot_credentials_conn_id

    def create_credentials(self, conn) -> Credential:
        """Creates DataRobot Basic Credentials using provided login/password."""
        if not conn.login:
            raise AirflowException("login is not defined")

        if not conn.password:
            raise AirflowException("password is not defined")

        self.log.info(f"Creating Basic Credentials:{self.datarobot_credentials_conn_id}")
        credential = Credential.create_basic(
            name=self.datarobot_credentials_conn_id,
            user=conn.login,
            password=conn.password,
            description=self.default_credential_description,
        )

        return credential

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "basic",
            "user": conn.login,
            "password": conn.password,
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext('DataRobot Connection'),
                widget=BS3TextFieldWidget(),
                default='datarobot_default',
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ['host', 'schema', 'port', 'extra'],
            "relabeling": {},
            "placeholders": {
                'datarobot_connection': 'datarobot_default',
                'login': '',
                'password': '',
            },
        }


class GoogleCloudCredentialsHook(CredentialsBaseHook):
    hook_name = 'DataRobot GCP Credentials'
    conn_type = 'datarobot.credentials.gcp'

    def create_credentials(self, conn) -> Credential:
        """Returns Google Cloud credentials for params in connection object"""

        gcp_key = conn.extra_dejson.get('gcp_key', '')

        if not gcp_key:
            raise AirflowException("gcp_key is not defined")

        try:
            self.log.info("Trying to parse provided GCP key json")
            # removing newlines and parsing json:
            gcp_key_json = json.loads(gcp_key.replace("\n", ""))
            self.log.info(f"Creating Google Cloud Credentials:{self.datarobot_credentials_conn_id}")
            credential = Credential.create_gcp(
                name=self.datarobot_credentials_conn_id,
                gcp_key=gcp_key_json,
                description=self.default_credential_description,
            )
            return credential

        except Exception as e:
            self.log.error(
                f"Error creating GCP Credentials: {self.datarobot_credentials_conn_id}, message:{str(e)}"
            )
            raise AirflowException(
                f"Error creating GCP Credentials: {self.datarobot_credentials_conn_id}"
            )

    def get_credential_data(self, conn) -> dict:
        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "gcp",
            "gcpKey": conn.extra_dejson.get('gcp_key', ''),
        }
        return credential_data

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "datarobot_connection": StringField(
                lazy_gettext('DataRobot Connection'),
                widget=BS3TextFieldWidget(),
                default='datarobot_default',
            ),
            "gcp_key": StringField(
                lazy_gettext('GCP Key (Service Account)'),
                widget=BS3TextAreaFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ['host', 'schema', 'port', 'login', 'password', 'extra'],
            "relabeling": {},
            "placeholders": {
                'datarobot_connection': 'datarobot_default',
                'gcp_key': 'Enter a valid JSON string',
            },
        }
