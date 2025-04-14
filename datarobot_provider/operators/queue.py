# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import List
from typing import Optional

import datarobot as dr
from airflow.utils.context import Context
from datarobot import QUEUE_STATUS

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class GetJobsOperator(BaseDatarobotOperator):
    """
    Get a list of jobs that match the requested status.

    Args:
        project_id (str): DataRobot project ID.
        status (str): Status of the job. Default is QUEUE_STATUS.INPROGRESS.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id", "job_id"]

    def __init__(
        self,
        *,
        project_id: str,
        status: Optional[str] = QUEUE_STATUS.INPROGRESS,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.status = status

    def validate(self):
        if not self.project_id:
            raise ValueError("project_id is required.")
        if self.status not in list(QUEUE_STATUS.__dict__.values()):
            raise ValueError(f"Invalid status: {self.status}, must be a QUEUE_STATUS.")

    def execute(self, context: Context) -> List[str]:
        project = dr.Project.get(self.project_id)
        # Actual filtering logic is handled by the DataRobot API
        project_jobs = project.get_all_jobs(status=self.status)

        return [str(job.id) for job in project_jobs]


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
