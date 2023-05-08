# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

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
    default_datarobot_credentials_conn_name = 'datarobot_credentials_default'
    hook_name = 'DataRobot Credentials'
    default_credential_description = "Credentials managed by Airflow provider for Datarobot"

    def __init__(
        self,
        datarobot_credentials_conn_id: str = default_datarobot_credentials_conn_name,
    ) -> None:
        super().__init__()
        self.datarobot_credentials_conn_id = datarobot_credentials_conn_id

    def create_credentials(self) -> Credential:
        """Creates DataRobot Credentials."""
        raise NotImplementedError()

    def get_credential_data(self) -> dict:
        """Creates credential data dict"""
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
                if self.default_credential_description == credential.description:
                    self.log.info(
                        f"Found Existing Credentials :{credential.name} , id={credential.credential_id}"
                    )
                    break
                else:
                    raise AirflowException(
                        f"Found Existing Credentials :{credential.name} , id={credential.credential_id}"
                        f"not managed by Airflow provider: {credential.description}"
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
        """Test DataRobot Credentials exist"""
        try:
            credential, credential_data = self.run()
            self.log.info(f"Checking credentials:{credential.credential_id} is created already")
            credential = Credential.get(credential.credential_id)
            if self.default_credential_description == credential.description:
                return True, f"Credentials exist, credential id={credential.credential_id}"
            else:
                return (
                    False,
                    f"Found Credentials, id={credential.credential_id} not managed by Airflow",
                )
        except Exception as e:
            return False, str(e)


class BasicCredentialsHook(CredentialsBaseHook):
    conn_type = 'datarobot_basic_credentials'

    def create_credentials(self, conn) -> Credential:
        """Creates DataRobot Basic Credentials using provided login/password."""
        if not conn.login:
            raise AirflowException("login is not defined")

        if not conn.password:
            raise AirflowException("password is not defined")

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
