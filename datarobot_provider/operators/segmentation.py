# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from datarobot import SegmentationTask
from datarobot.enums import DEFAULT_MAX_WAIT

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class CreateSegmentationTaskOperator(BaseDatarobotOperator):
    """
    Creates a new segmentation task for a model in a project. This operator will handle the process
    of registering the model and constructing a segmentation task from the resulting registered model
    with the given parameters.The task can then be used to start a new autopilot using the segmentation task.

    See the StartAutopilotOperator and StartDatetimeAutopilotOperator for more information.

    Args:
        project_id (str): DataRobot project ID.
        target (str): Target variable for the segmentation task.
        use_time_series (bool): Whether to use time series data. Defaults to True.
        datetime_partition_column (Optional[str]): Datetime partition column name.
        multiseries_id_columns (Optional[List[str]]): List of multiseries ID column names.
        user_defined_segment_id_columns (Optional[List[str]]): List of user-defined segment ID column names.
        max_wait (int): Maximum wait time for the task to complete. Defaults to DEFAULT_MAX_WAIT.
        model_package_id (Optional[str]): Model package ID to use for the segmentation task.

    Returns:
        str: ID of the segmentation task created.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        target: str,
        use_time_series: bool = True,
        datetime_partition_column: Optional[str] = None,
        multiseries_id_columns: Optional[List[str]] = None,
        user_defined_segment_id_columns: Optional[List[str]] = None,
        max_wait: int = DEFAULT_MAX_WAIT,
        model_package_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.target = target
        self.use_time_series = use_time_series
        self.datetime_partition_column = datetime_partition_column
        self.multiseries_id_columns = multiseries_id_columns
        self.user_defined_segment_id_columns = user_defined_segment_id_columns
        self.max_wait = max_wait
        self.model_package_id = model_package_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required to create a SegmentationTask.")

        if not self.target:
            raise ValueError("target is required to create a SegmentationTask.")

    def execute(self, context: Context) -> str:
        segmentation_task_results = dr.SegmentationTask.create(
            project_id=self.project_id,
            target=self.target,
            use_time_series=self.use_time_series,
            datetime_partition_column=self.datetime_partition_column,
            multiseries_id_columns=self.multiseries_id_columns,
            user_defined_segment_id_columns=self.user_defined_segment_id_columns,
            max_wait=self.max_wait,
            model_package_id=self.model_package_id,
        )
        completed_jobs = segmentation_task_results["completedJobs"]
        if len(completed_jobs) == 0:
            raise AirflowException("No completed jobs found in segmentation task results.")

        segmentation_task: SegmentationTask = completed_jobs[0]
        return segmentation_task.id
