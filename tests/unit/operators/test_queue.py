# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from datarobot import QUEUE_STATUS

from datarobot_provider.operators.queue import CancelJobOperator
from datarobot_provider.operators.queue import GetJobsOperator


@pytest.mark.parametrize(
    "project_id, status, expected_exception, match",
    [
        ("project-id", QUEUE_STATUS.INPROGRESS, None, None),
        (None, QUEUE_STATUS.INPROGRESS, ValueError, "project_id is required."),
        (
            "project-id",
            "INVALID_STATUS",
            ValueError,
            "Invalid status: INVALID_STATUS, must be a QUEUE_STATUS.",
        ),
    ],
)
def test_get_jobs_operator_validate(project_id, status, expected_exception, match):
    operator = GetJobsOperator(task_id="get_jobs", project_id=project_id, status=status)
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.queue.dr.Project.get")
def test_get_jobs_operator_execute(mock_get_project):
    mock_project = MagicMock()
    mock_job1 = MagicMock()
    mock_job1.id = "job-id-1"
    mock_job2 = MagicMock()
    mock_job2.id = "job-id-2"
    mock_project.get_all_jobs.return_value = [mock_job1, mock_job2]
    mock_get_project.return_value = mock_project

    operator = GetJobsOperator(
        task_id="get_jobs",
        project_id="project-id",
        status=QUEUE_STATUS.INPROGRESS,
    )
    result = operator.execute(context={})
    assert result == ["job-id-1", "job-id-2"]
    mock_get_project.assert_called_once_with("project-id")
    mock_project.get_all_jobs.assert_called_once_with(
        status=QUEUE_STATUS.INPROGRESS,
    )


@pytest.mark.parametrize(
    "project_id, job_id, expected_exception, match",
    [
        ("project-id", "job-id", None, None),
        (None, "job-id", ValueError, "project_id is required."),
        ("project-id", None, ValueError, "job_id is required."),
    ],
)
def test_cancel_job_operator_validate(project_id, job_id, expected_exception, match):
    operator = CancelJobOperator(task_id="cancel_job", project_id=project_id, job_id=job_id)
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.queue.dr.Project.get")
@patch("datarobot_provider.operators.queue.dr.Job.get")
def test_cancel_job_operator_execute(mock_get_job, mock_get_project):
    mock_project = MagicMock()
    mock_job = MagicMock()
    mock_job.status = QUEUE_STATUS.INPROGRESS
    mock_get_project.return_value = mock_project
    mock_get_job.return_value = mock_job

    operator = CancelJobOperator(task_id="cancel_job", project_id="project-id", job_id="job-id")
    operator.execute(context={})

    mock_get_project.assert_called_once_with("project-id")
    mock_get_job.assert_called_once_with("project-id", "job-id")
    mock_project.pause_autopilot.assert_called_once()
    mock_job.cancel.assert_called_once()
    mock_project.unpause_autopilot.assert_called_once()


@patch("datarobot_provider.operators.queue.dr.Project.get")
@patch("datarobot_provider.operators.queue.dr.Job.get")
def test_cancel_job_operator_execute_job_completed(mock_get_job, mock_get_project):
    mock_project = MagicMock()
    mock_job = MagicMock()
    mock_job.status = "COMPLETED"
    mock_get_project.return_value = mock_project
    mock_get_job.return_value = mock_job

    operator = CancelJobOperator(task_id="cancel_job", project_id="project-id", job_id="job-id")
    operator.execute(context={})

    mock_get_project.assert_called_once_with("project-id")
    mock_get_job.assert_called_once_with("project-id", "job-id")
    mock_project.pause_autopilot.assert_not_called()
    mock_job.cancel.assert_not_called()
    mock_project.unpause_autopilot.assert_not_called()
