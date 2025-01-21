# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any

import datarobot as dr
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from datarobot.errors import AsyncProcessUnsuccessfulError

from datarobot_provider.hooks.datarobot import DataRobotHook


class AutopilotCompleteSensor(BaseSensorOperator):
    """
    Checks if the DataRobot Autopilot has completed training the models.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed
    :rtype: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = ["project_id"]

    def __init__(
        self,
        *,
        project_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if Autopilot is complete")
        project = dr.Project.get(self.project_id)
        if project._autopilot_status_check()["autopilot_done"]:
            return True
        return False


class ScoringCompleteSensor(BaseSensorOperator):
    """
    Checks whether scoring predictions is complete.

    :param job_id: Batch prediction job ID
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

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if scoring is complete")
        job = dr.BatchPredictionJob.get(self.job_id)
        job_data = job.get_status()
        if job_data["status"].lower()[:5] in ["error", "abort"]:
            raise AsyncProcessUnsuccessfulError(
                f"The job did not complete successfully. Job Data: {job_data}"
            )
        if job_data["status"].lower() == "completed":
            return True
        return False
