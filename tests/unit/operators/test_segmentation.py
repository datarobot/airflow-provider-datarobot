# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from unittest.mock import MagicMock
from unittest.mock import patch

import datarobot as dr
import pytest
from airflow.exceptions import AirflowException

from datarobot_provider.operators.segmentation import CreateSegmentationTaskOperator


@pytest.mark.parametrize(
    "project_id, target, expected_exception, match",
    [
        ("project-id", "target", None, None),
        (None, "target", ValueError, "project_id is required to create a SegmentationTask."),
        ("project-id", None, ValueError, "target is required to create a SegmentationTask."),
    ],
)
def test_create_segmentation_task_operator_validate(project_id, target, expected_exception, match):
    operator = CreateSegmentationTaskOperator(
        task_id="create_segmentation_task", project_id=project_id, target=target
    )
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.segmentation.dr.SegmentationTask.create")
def test_create_segmentation_task_operator_execute(mock_create):
    mock_task = MagicMock()
    mock_task.id = "segmentation-task-id"
    mock_create.return_value = {"completedJobs": [mock_task]}

    operator = CreateSegmentationTaskOperator(
        task_id="create_segmentation_task", project_id="project-id", target="target"
    )
    result = operator.execute(context={})
    assert result == "segmentation-task-id"
    mock_create.assert_called_once_with(
        project_id="project-id",
        target="target",
        use_time_series=True,
        datetime_partition_column=None,
        multiseries_id_columns=None,
        user_defined_segment_id_columns=None,
        max_wait=dr.enums.DEFAULT_MAX_WAIT,
        model_package_id=None,
    )


@patch("datarobot_provider.operators.segmentation.dr.SegmentationTask.create")
def test_create_segmentation_task_operator_execute_no_completed_jobs(mock_create):
    mock_create.return_value = {"completedJobs": []}

    operator = CreateSegmentationTaskOperator(
        task_id="create_segmentation_task", project_id="project-id", target="target"
    )
    with pytest.raises(
        AirflowException, match="No completed jobs found in segmentation task results."
    ):
        operator.execute(context={})
