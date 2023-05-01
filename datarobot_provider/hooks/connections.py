# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict

import datarobot as dr
from airflow import AirflowException
from airflow.hooks.base import BaseHook

from datarobot_provider.hooks.datarobot import DataRobotHook


class JDBCDataSourceHook(BaseHook):
    """
    A hook that interacts with DataRobot via its public Python API library to
    manage JDBC connections with corresponding credentials.

    :param datarobot_jdbc_conn_id: Connection ID, defaults to `datarobot_jdbc_default`
    :type datarobot_jdbc_conn_id: str, optional
    """

    conn_name_attr = 'datarobot_jdbc_conn_id'
    default_datarobot_jdbc_conn_name = 'datarobot_jdbc_default'
    conn_type = 'datarobot_jdbc_datasource'
    hook_name = 'DataRobot JDBC DataSource'

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
            "jdbc_driver": StringField(
                lazy_gettext('JDBC Driver'),
                widget=BS3TextFieldWidget(),
                default='',
            ),
            "jdbc_url": StringField(
                lazy_gettext('JDBC URL'),
                widget=BS3TextFieldWidget(),
                default='jdbc:',
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
                'jdbc_driver': '',
                'jdbc_url': 'jdbc:',
                'login': '',
                'password': '',
            },
        }

    def __init__(
        self,
        datarobot_jdbc_conn_id: str = default_datarobot_jdbc_conn_name,
    ) -> None:
        super().__init__()
        self.datarobot_jdbc_conn_id = datarobot_jdbc_conn_id

    def get_conn(self) -> (dict, dr.DataStore):
        """Initializes a DataRobot DataStore instance with associated credentials."""

        conn = self.get_connection(self.datarobot_jdbc_conn_id)

        datarobot_connection_id = conn.extra_dejson.get('datarobot_connection', '')

        if not datarobot_connection_id:
            raise AirflowException("datarobot_connection is not defined")

        # Initialize DataRobot client by DataRobotHook
        DataRobotHook(datarobot_conn_id=datarobot_connection_id).run()

        jdbc_driver_name = conn.extra_dejson.get('jdbc_driver', '')

        if not jdbc_driver_name:
            raise AirflowException("jdbc_driver is not defined")

        jdbc_url = conn.extra_dejson.get('jdbc_url', '')

        if not jdbc_url:
            raise AirflowException("jdbc_url is not defined")

        if not conn.login:
            raise AirflowException("login is not defined")

        if not conn.password:
            raise AirflowException("password is not defined")

        # For methods that accept credential data instead of credential ID
        credential_data = {
            "credentialType": "basic",
            "user": conn.login,
            "password": conn.password,
        }

        # Find the JDBC driver ID from name:
        for jdbc_drv_item in dr.DataDriver.list():
            if jdbc_drv_item.canonical_name == jdbc_driver_name:
                jdbc_driver_id = jdbc_drv_item.id
                self.log.info(
                    f"Found JDBC Driver:{jdbc_drv_item.canonical_name} , id={jdbc_drv_item.id}"
                )
                break

        if jdbc_driver_id is None:
            raise AirflowException("JDBC Driver not found")

        data_store = None
        # Check if DataStore created already:
        for data_store_item in dr.DataStore.list():
            if data_store_item.canonical_name == self.datarobot_jdbc_conn_id:
                data_store = data_store_item
                self.log.info(
                    f"Found existing DataStore:{data_store.canonical_name} , id={data_store.id}"
                )
                break

        if data_store is None:
            self.log.info(
                f"DataStore:{self.datarobot_jdbc_conn_id} does not exist, trying to create it"
            )
            data_store = dr.DataStore.create(
                data_store_type='jdbc',
                canonical_name=self.datarobot_jdbc_conn_id,
                driver_id=jdbc_driver_id,
                jdbc_url=jdbc_url,
            )
            self.log.info(
                f"DataStore:{self.datarobot_jdbc_conn_id} successfully created, id={data_store.id}"
            )
        elif (
            jdbc_url != data_store.params.jdbc_url or jdbc_driver_id != data_store.params.driver_id
        ):
            self.log.info(f"Updating DataStore:{self.datarobot_jdbc_conn_id} with new params...")
            data_store.update(
                canonical_name=self.datarobot_jdbc_conn_id,
                driver_id=jdbc_driver_id,
                jdbc_url=jdbc_url,
            )
            self.log.info(
                f"DataStore:{self.datarobot_jdbc_conn_id} successfully updated, id={data_store.id}"
            )

        return credential_data, data_store

    def run(self) -> Any:
        # get Credentials and initialize DataStore object
        return self.get_conn()

    def test_connection(self):
        """Test DataRobot connection to JDBC DataSource"""
        try:
            credential_data, data_store = self.run()
            test_result = data_store.test(
                username=credential_data["user"], password=credential_data["password"]
            )

            return True, test_result['message']
        except Exception as e:
            return False, str(e)