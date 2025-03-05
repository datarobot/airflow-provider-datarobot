# Copyright 2025 DataRobot, Inc. and its affiliates.
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

from datarobot_provider.constants import DATAROBOT_CONN_ID
from datarobot_provider.hooks.datarobot import DataRobotHook


class WaitForPredictionsSensor(BaseSensorOperator):
    """
    Checks whether a predictions job is complete.

    Args:
        project_id (str): DataRobot project ID.
        predict_job_id (str): Predict job ID.

    Returns:
        bool: False if not yet completed, True when complete.
    """

    template_fields = ["project_id", "predict_job_id"]

    def __init__(
        self,
        *,
        project_id: str,
        predict_job_id: str,
        datarobot_conn_id: str = DATAROBOT_CONN_ID,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.predict_job_id = predict_job_id

        self.datarobot_conn_id = datarobot_conn_id
        self.hook = DataRobotHook(datarobot_conn_id)

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        try:
            self.log.info("Checking if PredictJob is finished.")
            dr.PredictJob.get(project_id=self.project_id, predict_job_id=self.predict_job_id)
        except dr.errors.PendingJobFinished:
            return True
        return False
