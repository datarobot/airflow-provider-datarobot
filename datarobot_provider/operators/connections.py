# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowNotFoundException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.connections import JDBCDataSourceHook
from datarobot_provider.hooks.datarobot import DataRobotHook


class GetOrCreateDataStoreOperator(BaseOperator):
    """
    Fetching DataStore by connection name or creating if it does not exist
    and return DataStore ID.

    :param connection_param_name: name of parameter in the config file corresponding to connection name
    :type connection_param_name: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Credentials ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = []
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        connection_param_name: str = "datarobot_connection_name",
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        self.connection_param_name = connection_param_name
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.connection_param_name not in context["params"]:
            raise AirflowNotFoundException(
                f"Attribute: {self.connection_param_name} not present in config"
            )
        # Getting connection name from config:
        connection_name = context["params"][self.connection_param_name]

        # Fetch stored JDBC Connection with credentials
        credential, credential_data, data_store = JDBCDataSourceHook(
            datarobot_credentials_conn_id=connection_name
        ).run()

        if data_store is not None:
            self.log.info(f"Found preconfigured jdbc connection: {connection_name}")

        return data_store.id
