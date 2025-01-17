# Copyright 2022 DataRobot, Inc. and its affiliates.
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


class MonitoringJobCompleteSensor(BaseSensorOperator):
    """
    Checks whether monitoring job is complete.

    :param job_id: Monitoring job ID
    :type job_id: str
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

    def poke(self, context: Dict[Any, Any]) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if batch monitoring job is complete")
        job = dr.BatchMonitoringJob.get(project_id="", job_id=self.job_id)
        job_data = job.get_status()
        if job_data["status"].lower()[:5] in ["error", "abort"]:
            raise AsyncProcessUnsuccessfulError(
                f"The job did not complete successfully. Job Data: {job_data}"
            )
        if job_data["status"].lower() == "completed":
            return True
        return False
