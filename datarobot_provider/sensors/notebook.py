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
from datarobot.rest import DataRobotClientConfig
from datarobot.rest import RESTClientObject

from datarobot_provider.hooks.datarobot import DataRobotHook


class NotebookJobCompleteSensor(BaseSensorOperator):
    """
    Checks whether monitoring job is complete.

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
        dr_client = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # TODO: Is this hacky? Using the token & domain from the _other_ dr_client as part of Airflow guts
        api_gw_endpoint = f"{dr_client.domain}/api-gw/nbx"
        api_gw_client = RESTClientObject.from_config(
            DataRobotClientConfig(endpoint=api_gw_endpoint, token=dr_client.token)
        )

        orch_url = f"orchestrator/notebooks/{self.notebook_id}"

        exec_status_resp = api_gw_client.get(f"{orch_url}/executionStatus/")
        assert exec_status_resp.status_code == 200, (
            exec_status_resp.status_code,
            exec_status_resp.text,
        )
        exec_status = exec_status_resp.json()["status"]
        if exec_status == "idle":
            self.log.info("Notebook execution status now idle.")

            # Now stop the notebook
            stop_response = api_gw_client.post(url=f"{orch_url}/stop/")
            self.log.info(f"{stop_response.status_code=}")

            # Return True, we're all done
            return True

        return False
