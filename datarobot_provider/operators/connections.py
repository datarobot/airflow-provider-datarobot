# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowNotFoundException
from airflow.utils.context import Context

from datarobot_provider.hooks.connections import JDBCDataSourceHook
from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class GetOrCreateDataStoreOperator(BaseDatarobotOperator):
    """
    !! Please, manage database connections via DataRobot rather than Airflow and use *GetDataStoreOperator* instead. !!

    Fetching DataStore by connection name or creating if it does not exist
    and return DataStore ID.

    :param connection_param_name: name of parameter in the config file corresponding to connection name
    :type connection_param_name: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Credentials ID
    :rtype: str
    """

    def __init__(
        self,
        *,
        connection_param_name: str = "datarobot_connection_name",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_param_name = connection_param_name

    def execute(self, context: Context) -> str:
        if self.connection_param_name not in context["params"]:
            raise AirflowNotFoundException(
                f"Attribute: {self.connection_param_name} not present in config"
            )
        # Getting connection name from config:
        connection_name = context["params"][self.connection_param_name]

        # Fetch stored JDBC Connection with credentials
        _, _, data_store = JDBCDataSourceHook(datarobot_credentials_conn_id=connection_name).run()

        if data_store is not None:
            self.log.info(f"Found preconfigured jdbc connection: {connection_name}")

        return data_store.id


class GetDataStoreOperator(BaseDatarobotOperator):
    """Get a DataRobot data store id by data connection name.
    You have to create a DataRobot data connection in advance at /account/data-connections page.

    :param data_connection: unique, case-sensitive data connection name as you can see it at DataRobot.
    :type data_connection: str
    :return: Data store ID.
    :rtype: str
    """

    template_fields: Sequence[str] = ["data_connection"]

    def __init__(
        self,
        *,
        data_connection: str = "{{ params.data_connection }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.data_connection = data_connection

    def execute(self, context: Context) -> Any:
        for datastore in dr.DataStore.list(name=self.data_connection):
            if datastore.canonical_name == self.data_connection:
                break

        else:
            raise AirflowException(f"Connection {self.data_connection} was not found.")

        return datastore.id
