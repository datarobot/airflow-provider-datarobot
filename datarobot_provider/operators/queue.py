# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any

import datarobot as dr
from airflow.utils.context import Context
from datarobot import QUEUE_STATUS

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class CancelJobOperator(BaseDatarobotOperator):
    """
    Cancel a job that is in the queue or currently running.

    Args:
        project_id (str): DataRobot project ID.
        job_id (str): Job ID in the project.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id", "job_id"]

    def __init__(
        self,
        *,
        project_id: str,
        job_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_id = job_id

    def validate(self):
        if not self.project_id:
            raise ValueError("project_id is required.")
        if not self.job_id:
            raise ValueError("job_id is required.")

    def execute(self, context: Context) -> None:
        project = dr.Project.get(self.project_id)
        job = dr.Job.get(self.project_id, self.job_id)
        if job.status in [
            QUEUE_STATUS.INPROGRESS,
            QUEUE_STATUS.QUEUE,
        ]:
            project.pause_autopilot()
            job.cancel()
            project.unpause_autopilot()

        else:
            self.log.info("Job already completed or aborted.")
