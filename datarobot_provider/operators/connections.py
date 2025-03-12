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
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


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
