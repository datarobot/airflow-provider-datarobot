# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict

from airflow.sensors.base import BaseSensorOperator
from datarobot import FeatureImpactJob
from datarobot.errors import AsyncProcessUnsuccessfulError

from datarobot_provider.hooks.datarobot import DataRobotHook


class ComputePredictionExplanationsSensor(BaseSensorOperator):
    """
    Checks whether Compute Prediction Explanations job is complete.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param job_id: Compute Prediction Explanations job ID
    :type job_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed
    :rtype: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = ["project_id", "job_id"]

    def __init__(
        self,
        *,
        project_id: str,
        job_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_id = job_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Dict[Any, Any]) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if Compute Prediction Explanations job is complete")

        job = FeatureImpactJob.get(project_id=self.project_id, job_id=self.job_id)

        if job.status.lower() in ["error", "abort"]:
            raise AsyncProcessUnsuccessfulError(
                f"The job did not complete successfully. Job Status: {job.status}"
            )
        if job.status.lower() == "completed":
            return True
        return False
