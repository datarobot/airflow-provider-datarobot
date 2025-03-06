# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Union

from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.base import PokeReturnValue
from airflow.utils.context import Context
from datarobot import Job
from datarobot.errors import AsyncProcessUnsuccessfulError

from datarobot_provider.constants import DATAROBOT_CONN_ID
from datarobot_provider.hooks.datarobot import DataRobotHook


class DataRobotJobSensor(BaseSensorOperator):
    """
    Checks whether DataRobot Job is complete.

    Args:
        project_id (str): DataRobot project ID.
        job_id (str): DataRobot Job ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        bool: False if not yet completed.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = ["project_id", "job_id"]

    def __init__(
        self,
        *,
        project_id: str,
        job_id: str,
        datarobot_conn_id: str = DATAROBOT_CONN_ID,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_id = job_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def get_job_result(self, context: Context) -> Union[bool, PokeReturnValue]:
        # Default implementation return True if job is completed:
        return True

    def poke(self, context: Context) -> Union[bool, PokeReturnValue]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if DataRobot Job is complete")

        job = Job.get(project_id=self.project_id, job_id=self.job_id)

        if job.status.lower() in ["error", "abort"]:
            raise AsyncProcessUnsuccessfulError(
                f"The job did not complete successfully. Job Status: {job.status}"
            )
        if job.status.lower() == "completed":
            return self.get_job_result(context)
        return False
