# Copyright 2023 DataRobot, Inc. and its affiliates.
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
from airflow.sensors.base import BaseSensorOperator

from datarobot_provider.hooks.datarobot import DataRobotHook


class BaseAsyncResolutionSensor(BaseSensorOperator):
    """
    Checks if the DataRobot Async API call has completed.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed
    :rtype: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = ["async_location"]

    def __init__(
        self,
        *,
        async_location: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.async_location = async_location
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Dict[Any, Any]) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if not self.async_location:
            raise AirflowException("async location status link is not defined")

        self.log.info("Checking if DataRobot API async call is complete")
        async_status_check = dr.client.get_async_resolution_status(self.async_location)
        self.log.debug(f"API async call status:{async_status_check}")
        return async_status_check is not None
