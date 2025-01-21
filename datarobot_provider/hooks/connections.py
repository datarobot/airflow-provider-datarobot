# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any

from airflow.exceptions import AirflowException
from datarobot import Credential
from datarobot.models.data_store import DataStore
from datarobot.models.driver import DataDriver

from datarobot_provider.hooks.credentials import BasicCredentialsHook
from datarobot_provider.hooks.datarobot import DataRobotHook


class JDBCDataSourceHook(BasicCredentialsHook):
    """
    A hook that interacts with DataRobot via its public Python API library to
    manage JDBC connections with corresponding credentials.
    """

    conn_type = "datarobot.datasource.jdbc"
    hook_name = "DataRobot JDBC DataSource"

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
            "jdbc_driver": StringField(
                lazy_gettext("JDBC Driver"),
                widget=BS3TextFieldWidget(),
                default="",
            ),
            "jdbc_url": StringField(
                lazy_gettext("JDBC URL"),
                widget=BS3TextAreaFieldWidget(),
                default="jdbc:",
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
                "jdbc_driver": "",
                "jdbc_url": "jdbc:",
            },
        }

    def get_conn(self) -> tuple[Credential, dict, DataStore]:
        """Retrieving corresponding DataStore object or creating if not exist,
        updating it with new parameters in case of changes."""

        if not self.datarobot_credentials_conn_id:
            raise AirflowException("datarobot_credentials_conn_id is not defined")

        conn = self.get_connection(self.datarobot_credentials_conn_id)

        datarobot_connection_id = conn.extra_dejson.get("datarobot_connection", "")

        if not datarobot_connection_id:
            raise AirflowException("datarobot_connection is not defined")

        # Initialize DataRobot client by DataRobotHook
        DataRobotHook(datarobot_conn_id=datarobot_connection_id).run()

        jdbc_driver_name = conn.extra_dejson.get("jdbc_driver", "")

        if not jdbc_driver_name:
            raise AirflowException("jdbc_driver is not defined")

        jdbc_url = conn.extra_dejson.get("jdbc_url", "")

        if not jdbc_url:
            raise AirflowException("jdbc_url is not defined")

        if not conn.login:
            raise AirflowException("login is not defined")

        if not conn.password:
            raise AirflowException("password is not defined")

        credential = self.get_or_create_credential(conn)

        # For methods that accept credential data instead of credential ID
        credential_data = self.get_credential_data(conn)

        # Find the JDBC driver ID from name:
        for jdbc_drv_item in DataDriver.list():
            if jdbc_drv_item.canonical_name == jdbc_driver_name:
                jdbc_driver_id = jdbc_drv_item.id
                self.log.info(
                    f"Found JDBC Driver:{jdbc_drv_item.canonical_name} , id={jdbc_drv_item.id}"
                )
                break
        else:
            raise AirflowException(f'JDBC Driver "{jdbc_driver_name}" not found')

        # Check if DataStore created already:
        for data_store in DataStore.list():
            if data_store.canonical_name == self.datarobot_credentials_conn_id:
                self.log.info(
                    f"Found existing DataStore:{data_store.canonical_name} , id={data_store.id}"
                )
                break
        else:
            if not self.datarobot_credentials_conn_id:
                raise AirflowException("datarobot_credentials_conn_id is not defined")

            self.log.info(
                f"DataStore:{self.datarobot_credentials_conn_id} does not exist, trying to create it"
            )
            data_store = DataStore.create(
                data_store_type="jdbc",
                canonical_name=self.datarobot_credentials_conn_id,
                driver_id=jdbc_driver_id,
                jdbc_url=jdbc_url,
            )
            self.log.info(
                f"DataStore:{self.datarobot_credentials_conn_id} successfully created, id={data_store.id}"
            )

        if not data_store.params:
            raise AirflowException(f"DataStore:{self.datarobot_credentials_conn_id} has no params")

        if jdbc_url != data_store.params.jdbc_url or jdbc_driver_id != data_store.params.driver_id:
            self.log.info(
                f"Updating DataStore:{self.datarobot_credentials_conn_id} with new params..."
            )
            data_store.update(
                canonical_name=self.datarobot_credentials_conn_id,
                driver_id=jdbc_driver_id,
                jdbc_url=jdbc_url,
            )
            self.log.info(
                f"DataStore:{self.datarobot_credentials_conn_id} successfully updated, id={data_store.id}"
            )

        return credential, credential_data, data_store

    def run(self) -> Any:
        # get or create DataStore object updated with actual parameters
        return self.get_conn()

    def test_connection(self):
        """Test DataRobot connection to JDBC DataSource"""
        try:
            credential, credential_data, data_store = self.run()

            test_result = data_store.test(
                username=credential_data["user"], password=credential_data["password"]
            )

            success_massage = f"{test_result['message']}"
            return True, success_massage
        except Exception as e:
            return False, str(e)
