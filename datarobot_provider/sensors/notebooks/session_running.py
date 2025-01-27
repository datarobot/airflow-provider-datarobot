# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from datarobot._experimental.models.notebooks.notebook import Notebook

from datarobot_provider.hooks.datarobot import DataRobotHook


class NotebookSessionRunningSensor(BaseSensorOperator):
    """
    Checks if the notebook session is running.

    :param notebook_id: Notebook ID
    :type notebook_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed
    :rtype: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = ["notebook_id"]

    def __init__(
        self,
        *,
        notebook_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        notebook = Notebook.get(self.notebook_id)
        self.log.info("Checking if notebook session is running.")
        return notebook.is_running()
