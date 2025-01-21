# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from datarobot.errors import AsyncProcessUnsuccessfulError
from datarobot.models.status_check_job import StatusCheckJob

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
    template_fields = ["job_id"]

    def __init__(
        self,
        *,
        job_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if not self.job_id:
            raise AirflowException("job_id is not defined")

        self.log.info("Checking if DataRobot API async call is complete")

        status_check_job = StatusCheckJob.from_id(self.job_id)
        job_status_result = status_check_job.get_status()

        self.log.debug(f"API async call status:{job_status_result.status}")
        self.log.debug(
            f"API async call completed_resource_url:{job_status_result.completed_resource_url}"
        )

        if job_status_result.status in ["ERROR", "ABORT"]:
            raise AsyncProcessUnsuccessfulError(
                f"The job did not complete successfully. Job Data: {job_status_result.status}"
            )
        elif job_status_result.status == "COMPLETED":
            return True
        return False
