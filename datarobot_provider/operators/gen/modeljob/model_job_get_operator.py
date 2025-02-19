# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context
from datarobot.models.modeljob import ModelJob

from datarobot_provider.constants import DATAROBOT_CONN_ID
from datarobot_provider.hooks.datarobot import DataRobotHook


class ModelJobGetOperator(BaseOperator):
    """Fetches one ModelJob. If the job finished, raises PendingJobFinished exception.

    :param project_id: The identifier of the project the model belongs to
    :type project_id: str
    :param model_job_id: The identifier of the model_job
    :type model_job_id: str
    :return: model_job
    :rtype ModelJob
    """

    template_fields: Sequence[str] = ["project_id", "model_job_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        model_job_id: Optional[str] = None,
        datarobot_conn_id: str = DATAROBOT_CONN_ID,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_job_id = model_job_id
        self.datarobot_conn_id = datarobot_conn_id

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.project_id is None:
            raise ValueError("project_id is required for ModelJob.")
        if self.model_job_id is None:
            raise ValueError("model_job_id is required for ModelJob.")

        result = ModelJob.get(
            self.project_id,
            self.model_job_id,
        )
        return result.id
