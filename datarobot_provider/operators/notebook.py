# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import time
from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datarobot.rest import DataRobotClientConfig
from datarobot.rest import RESTClientObject

from datarobot_provider.hooks.datarobot import DataRobotHook

ParametersType = list[dict[str, str]] | None


# TODO: Separate operators for Standalone versus Codespace?
class NotebookOperator(BaseOperator):
    """
    Runs a DataRobot Notebook.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param parameters: TODO: Fill in ...
    :type parameters: TODO: Fill in ...
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: TODO ...
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "notebook_id",
    ]

    def __init__(
        self,
        *,
        notebook_id: str,
        parameters: ParametersType = None,
        datarobot_conn_id: str = "datarobot_default",  # TODO: Make this a constant somewhere
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        """
        TODO:
        - Decide when/where to use Airflow Sensors
        - Break this out, at minimum, in to methods but also maybe separate, small operators for each action
        """

        # Initialize DataRobot client
        dr_client = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # TODO: Is this hacky? Using the token & domain from the _other_ dr_client as part of Airflow guts
        api_gw_endpoint = f"{dr_client.domain}/api-gw/nbx"
        api_gw_client = RESTClientObject.from_config(
            DataRobotClientConfig(endpoint=api_gw_endpoint, token=dr_client.token)
        )

        self.log.info(f"Using client {dr_client=}")
        self.log.info(f"Using client {dr_client.domain=}")

        # Parameters is possibly None hence using .get()
        # TODO - Clean this up.
        if context["params"]:
            parameters = context["params"].get("notebook_parameters", {}).get("data")
        else:
            parameters = None

        # Run the notebook
        self.log.info(f"Trying to run Notebook {self.notebook_id=}")
        self.log.info(f"Using parameters {parameters=}")

        orch_url = f"orchestrator/notebooks/{self.notebook_id}"
        self.log.info(f"Using URL {orch_url=}")

        # TODO: Maybe alter the naming of this param in notebooks code - can't send Env Vars w/out it set to True
        start_payload = {"isScheduledRun": True}
        if parameters:
            start_payload["parameters"] = parameters

        start_response = api_gw_client.post(
            url=f"{orch_url}/start/",
            data=start_payload,
        )
        assert start_response.status_code == 200, (start_response.status_code, start_response.text)

        # We need to wait for the session to start before executing code (session status of "running")
        # Waiting 10 minutes (600 seconds) - TODO: Make this configurable
        for _ in range(600):
            status_response = api_gw_client.get(orch_url)
            assert status_response.status_code == 200, (
                status_response.status_code,
                status_response.text,
            )
            session_status = status_response.json()["status"]
            if session_status == "running":
                self.log.info("Notebook session now running.")
                break
            self.log.info(f"Notebook session not yet running {session_status=}")
            time.sleep(1)

        # An execution request can contain 0-n cell ids for a given notebook. If no cell ids are given, this means
        # that the whole notebook will get executed in the order that the cells are saved.
        # `data` (as a dict) can include a list for `cellIds` if executing only specific cells
        data = None
        execution_response = api_gw_client.post(f"{orch_url}/execute/", json=data)
        assert execution_response.status_code == 202, (
            execution_response.status_code,
            execution_response.text,
        )

        return self.notebook_id

        # BELOW CODE works as-is _without_ using Sensors

        # # TODO: Make this either configurable and/or `while True:` infinite loop
        # # We wait for execution of the notebook to be complete (execution status of "idle")
        # minutes_to_wait = 10  # Adjust this accordingly if your notebook takes longer to run
        # for _ in range(minutes_to_wait * 60):
        #     exec_status_resp = api_gw_client.get(f"{orch_url}/executionStatus/")
        #     assert exec_status_resp.status_code == 200, (
        #         exec_status_resp.status_code,
        #         exec_status_resp.text,
        #     )
        #     exec_status = exec_status_resp.json()["status"]
        #     if exec_status == "idle":
        #         self.log.info("Notebook execution status now idle.")
        #         break
        #     self.log.info(f"Notebook still executing {exec_status=}")
        #     time.sleep(1)  # Adjust this to poll less frequently than once a second

        # # Now stop the notebook
        # stop_response = api_gw_client.post(url=f"{orch_url}/stop/")
        # self.log.info(f"{stop_response.status_code=}")

        # # TODO: Decide what to return here
        # return "TODO"
