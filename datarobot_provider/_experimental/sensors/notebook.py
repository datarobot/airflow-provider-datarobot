# Copyright 2025 DataRobot, Inc. and its affiliates.
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

try:
    from datarobot.models.notebooks.enums import ScheduledRunStatus
    from datarobot.models.notebooks.notebook import Notebook
    from datarobot.models.notebooks.scheduled_job import NotebookScheduledJob
except ImportError:
    from datarobot._experimental.models.notebooks.enums import ScheduledRunStatus
    from datarobot._experimental.models.notebooks.notebook import Notebook
    from datarobot._experimental.models.notebooks.scheduled_job import NotebookScheduledJob

from datarobot_provider.constants import DATAROBOT_CONN_ID
from datarobot_provider.hooks.datarobot import DataRobotHook


class NotebookRunCompleteSensor(BaseSensorOperator):
    """
    Checks if the notebook run is complete.

    :param notebook_id: Notebook ID
    :type notebook_id: str
    :param job_id: Job ID
    :type job_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed
    :rtype: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        "notebook_id",
        "job_id",
    ]

    def __init__(
        self,
        *,
        notebook_id: str = "{{ params.notebook_id }}",
        job_id: str,
        datarobot_conn_id: str = DATAROBOT_CONN_ID,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.job_id = job_id
        self.datarobot_conn_id = datarobot_conn_id

        self.hook = DataRobotHook(datarobot_conn_id)

    def _construct_results_url(self, domain: str, use_case_id: str, revision_id: str) -> str:
        return (
            f"{domain}/usecases/{use_case_id}/overview/notebooks/run-history"
            f"?notebookId={self.notebook_id}"
            f"&revisionId={revision_id}"
        )

    def poke(self, context: Context) -> bool:
        # Initialize DataRobot client
        dr_client = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Checking if notebook execution is complete.")
        notebook = Notebook.get(self.notebook_id)

        # This should not happen but to keep mypy happy as well as remain very defensive we'll check
        if not notebook.use_case_id:
            raise AirflowException("Notebook should have use_case_id")

        notebook_scheduled_job = NotebookScheduledJob.get(
            use_case_id=notebook.use_case_id, scheduled_job_id=self.job_id
        )
        manual_run = notebook_scheduled_job.get_most_recent_run()

        if manual_run and manual_run.status in ScheduledRunStatus.terminal_statuses():
            self.log.info(f"Notebook job no longer running - status: {manual_run.status}")
            if manual_run.revision and manual_run.revision.id:
                results_url = self._construct_results_url(
                    dr_client.domain, notebook.use_case_id, manual_run.revision.id
                )
                self.log.info(f"To view results please visit: {results_url}")
            return True

        status_message = f" - status: {manual_run.status}" if manual_run else ""
        self.log.info(f"Notebook job still running{status_message}")
        return False
