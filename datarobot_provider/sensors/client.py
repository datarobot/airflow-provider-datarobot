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
from airflow.sensors.base import BaseSensorOperator
from datarobot.errors import AsyncProcessUnsuccessfulError

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
        client = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()
        self.log.info("Checking if DataRobot API async call is complete")

        join_endpoint = not self.async_location.startswith("http")  # Accept full qualified and relative urls
        response = client.get(self.async_location, allow_redirects=False, join_endpoint=join_endpoint)
        print(response)
        if response.status_code not in (200, 303):
            e_template = "The server gave an unexpected response. Status Code {}: {}"
            print(e_template)
            #raise errors.AsyncFailureError(e_template.format(response.status_code, response.text))
        #is_successful = success_fn(response)
        #if is_successful:
        #    return is_successful

        return False
